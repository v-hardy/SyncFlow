import sqlite3
import socket
from pathlib import Path


from hashutil import sha256_file
from fsutil import copy_file, move_file

MACHINE = socket.gethostname()


class EngineSync:
    # <======================================= INICIAR OBJETO =======================================>
    def __init__(self, pc_root: Path, usb_root: Path, db_name: str):
        self.pc_root = pc_root.resolve()
        self.usb_root = usb_root.resolve()
        self.db_pc_path = self.pc_root / ".sync" / db_name
        self.db_usb_path = self.usb_root / db_name
        self.temp_db_path = self.usb_root / f"{db_name}.tmp"

        # Asegurar carpeta oculta en PC
        (self.pc_root / ".sync").mkdir(exist_ok=True)

    # <======================================= INICIALIZAR DB =======================================>
    def create_schema(self, conn):
        # Definimos la ruta al archivo .sql
        # (Asumiendo que está en la misma carpeta que tu script)
        sql_file_path = Path(__file__).parent / "schema.sql"

        try:
            # Leemos el archivo SQL
            with open(sql_file_path, "r", encoding="utf-8") as f:
                sql_script = f.read()

            # Ejecutamos todo el contenido
            conn.executescript(sql_script)
            conn.commit()

        except FileNotFoundError:
            print(f"Error: No se encontró el archivo {sql_file_path}")
        except sqlite3.Error as e:
            print(f"Error de SQLite al crear el esquema: {e}")

    # <======================================= ESTABLECER CONEXION A DB =======================================>
    def get_db_connection(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row  # se devuelven como objetos tipo sqlite3.Row, que funcionan como un diccionario + tupla híbrido. Se accede a las columnas tanto por índice como por nombre, más legible y seguro
        self.create_schema(conn)  # Creo tabla de registros SQL solo si no existe aun
        return conn  # Retorno objeto conexion

    # <======================================= DESCARGAR CAMBIOS =======================================>
    def sync_from_usb(self, log_fn=print):
        usb_conn = self.get_db_connection(self.usb_db)
        loc_conn = self.get_db_connection(self.local_db)

        with loc_conn:
            loc_conn.execute("BEGIN IMMEDIATE")

            usb_rows = usb_conn.execute(
                "SELECT init_hash, content_hash, rel_path, timestamp FROM files"
            )

            for u in usb_rows:
                init_hash = u["init_hash"]
                usb_path = self.usb_root / u["rel_path"]

                local_row = loc_conn.execute(
                    "SELECT content_hash, rel_path FROM files WHERE init_hash=?",
                    (init_hash,),
                ).fetchone()

                # =========================
                # CASO: solo en USB
                # =========================
                if local_row is None:
                    dst = self.pc_root / u["rel_path"]
                    copy_file(usb_path, dst)

                    new_hash = sha256_file(dst)
                    if new_hash != u["content_hash"]:
                        raise RuntimeError(f"Hash mismatch (NEW_FROM_USB): {init_hash}")

                    loc_conn.execute(
                        """INSERT INTO files VALUES (?,?,?,?)""",
                        (init_hash, new_hash, u["rel_path"], u["timestamp"]),
                    )

                    log_fn(f"[NEW_FROM_USB] {init_hash}")
                    continue

                # =========================
                # CASO: existe en ambos
                # =========================
                same_hash = u["content_hash"] == local_row["content_hash"]
                same_path = u["rel_path"] == local_row["rel_path"]

                src = usb_path
                dst = self.pc_root / u["rel_path"]

                if same_hash and same_path:
                    continue

                if same_hash and not same_path:
                    old = self.pc_root / local_row["rel_path"]
                    move_file(old, dst)

                    loc_conn.execute(
                        "UPDATE files SET rel_path=? WHERE init_hash=?",
                        (u["rel_path"], init_hash),
                    )

                    log_fn(f"[MOVE_LOCAL] {init_hash}")
                    continue

                # contenido distinto → UPDATE
                copy_file(src, dst)
                new_hash = sha256_file(dst)

                if new_hash != u["content_hash"]:
                    raise RuntimeError(f"Hash mismatch (UPDATE): {init_hash}")

                if not same_path:
                    old = self.pc_root / local_row["rel_path"]
                    if old.exists():
                        old.unlink()

                loc_conn.execute(
                    """UPDATE files
                    SET content_hash=?, rel_path=?, timestamp=?
                    WHERE init_hash=?""",
                    (new_hash, u["rel_path"], u["timestamp"], init_hash),
                )

                log_fn(f"[UPDATE_LOCAL] {init_hash}")

        usb_conn.close()
        loc_conn.close()
