"""
SQL debe encargarse de:
CREATE / MODIFY / MOVE / DELETE lógicos
Orden de aplicación (op_time)
Evitar duplicados
Idempotencia
Resolución de conflictos (si luego la agregás)
Saber qué hay que hacer, no cómo
"""

import sqlite3
from pathlib import Path
import time

""" 
    def connect()
    def insert_movement()
    def get_pending_movements()
"""


class DB:
    def __init__(self, pc_root: Path, usb_root: Path, db_name: str):
        self.pc_root = pc_root.resolve()
        self.usb_root = usb_root.resolve()
        self.db_pc_path = self.pc_root / ".sync" / db_name
        self.db_temp_path = self.pc_root / ".sync" / f"{db_name}.tmp"
        self.db_usb_path = self.usb_root / db_name

        # Asegurar carpeta oculta en PC
        (self.pc_root / ".sync").mkdir(exist_ok=True)

    # <======================================= INICIALIZA DB =======================================>
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

    # <======================================= VERIFICA TABLA VACIA =======================================>
    def table_is_empty(self, conn, table):
        cur = conn.execute(f"SELECT 1 FROM {table} LIMIT 1")
        return cur.fetchone() is None

    # <======================================= ESTABLECE CONEXION =======================================>
    def get_db_connection(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row  # se devuelven como objetos tipo sqlite3.Row, que funcionan como un diccionario + tupla híbrido. Se accede a las columnas tanto por índice como por nombre, más legible y seguro
        self.create_schema(conn)  # Creo tabla de registros SQL solo si no existe aun
        return conn  # Retorno objeto conexion

    # <======================================= LEE =======================================>
    def read_states(self, conn):
        cursor = conn.execute(
            """
            SELECT
                init_hash,
                rel_path,
                content_hash,
                last_op_time,
                last_machine
            FROM master_states
            ORDER BY rel_path ASC
            """
        )

        master_states = []
        for row in cursor.fetchall():
            mov = {
                "init_hash": row["init_hash"],
                "rel_path": row["rel_path"],
                "content_hash": row["content_hash"],
                "last_op_time": row["last_op_time"],
                "last_machine": row["last_machine"],
            }
            master_states.append(mov)

        return master_states

    def read_movements(self, conn):
        cursor = conn.execute(
            """
            SELECT
                id,
                op_type,
                init_hash,
                rel_path,
                new_rel_path,
                content_hash,
                op_time,
                machine_name
            FROM movements
            ORDER BY rel_path ASC
            """
        )

        movements = []
        for row in cursor.fetchall():
            mov = {
                "id": row["id"],
                "op_type": row["op_type"],
                "init_hash": row["init_hash"],
                "rel_path": row["rel_path"],
                "new_rel_path": row["new_rel_path"],
                "content_hash": row["content_hash"],
                "op_time": row["op_time"],
                "machine_name": row["machine_name"],
            }
            movements.append(mov)

        return movements

    def read_tombstones(self, conn):
        cursor = conn.execute(
            """
                SELECT
                    init_hash,
                    content_hash,
                    deleted_at,
                    machine_name
                FROM tombstones
                ORDER BY init_hash ASC
                """
        )

        tombstones = []
        for row in cursor.fetchall():
            mov = {
                "init_hash": row["init_hash"],
                "content_hash": row["content_hash"],
                "deleted_at": row["deleted_at"],
                "machine_name": row["machine_name"],
            }
            tombstones.append(mov)

        return tombstones

    # <======================================= ACTUALIZA =======================================>
    def update_state(self, conn, mov: dict):
        op = mov["op_type"]

        if op == "CREATE":
            conn.execute(
                "INSERT INTO master_states (init_hash, rel_path, content_hash) VALUES (?, ?, ?)",
                (mov["init_hash"], mov["rel_path"], mov["content_hash"]),
            )

        elif op == "MODIFY":
            conn.execute(
                "UPDATE master_states SET content_hash = ? WHERE rel_path = ?",
                (mov["content_hash"], mov["rel_path"]),
            )

        elif op == "MOVE":
            conn.execute(
                """
                UPDATE master_states
                SET rel_path = ?
                WHERE rel_path = ?
                """,
                (mov["new_rel_path"], mov["rel_path"]),
            )

        elif op == "DELETE":
            conn.execute(
                "DELETE FROM master_states WHERE rel_path = ?", (mov["rel_path"],)
            )

    def upsert_movements(self, conn, mov: dict, log_fn=print):
        conn.execute(
            """
            INSERT OR REPLACE INTO movements (
                id, op_type, init_hash, rel_path, new_rel_path,
                content_hash, op_time, machine_name, applied_time
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                mov["id"],
                mov["op_type"],
                mov["init_hash"],
                mov["rel_path"],
                mov["new_rel_path"],
                mov["content_hash"],
                mov["op_time"],
                mov["machine_name"],
                int(time.time()),
            ),
        )
        log_fn(f"[UPDATE_MOVEMENTS] {mov['init_hash']}")

    # <======================================= ARCHIVA =======================================>
    def archive_and_delete_movement(self, conn, mov: dict):
        conn.execute(
            """
            INSERT INTO movements_history (
                id, op_type, init_hash, rel_path, new_rel_path,
                content_hash, op_time, machine_name, applied_time
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                mov["id"],
                mov["op_type"],
                mov["init_hash"],
                mov["rel_path"],
                mov["new_rel_path"],
                mov["content_hash"],
                mov["op_time"],
                mov["machine_name"],
                int(time.time()),
            ),
        )

        conn.execute(
            """
            INSERT INTO tombstones (
                init_hash,
                content_hash,
                deleted_at,
                machine_name
            )
            VALUES (?, ?, ?, ?)
        """,
            (
                mov["init_hash"],
                mov["content_hash"],
                mov["op_time"],
                mov["machine_name"],
            ),
        )

        conn.execute("DELETE FROM movements WHERE id = ?", (mov["id"],))

    # # <======================================= DESCARGAR CAMBIOS =======================================>
    # def sync_from_usb(self, log_fn=print):
    #     loc_conn = self.get_db_connection(self.db_pc_path)
    #     usb_conn = self.get_db_connection(
    #         self.db_usb_path
    #     )  # si no hay DB en usb, deberia omitir el resto de la funcion

    #     with loc_conn:
    #         loc_conn.execute("BEGIN IMMEDIATE")

    #         usb_rows = usb_conn.execute(
    #             "SELECT init_hash, content_hash, rel_path, timestamp FROM files"
    #         )

    #         for u in usb_rows:
    #             init_hash = u["init_hash"]
    #             usb_path = self.usb_root / u["rel_path"]

    #             local_row = loc_conn.execute(
    #                 "SELECT content_hash, rel_path FROM files WHERE init_hash=?",
    #                 (init_hash,),
    #             ).fetchone()

    #             # =========================
    #             # CASO: solo en USB
    #             # =========================
    #             if local_row is None:
    #                 dst = self.pc_root / u["rel_path"]
    #                 copy_file(usb_path, dst)

    #                 new_hash = sha256_file(dst)
    #                 if new_hash != u["content_hash"]:
    #                     raise RuntimeError(f"Hash mismatch (NEW_FROM_USB): {init_hash}")

    #                 loc_conn.execute(
    #                     """INSERT INTO files VALUES (?,?,?,?)""",
    #                     (init_hash, new_hash, u["rel_path"], u["timestamp"]),
    #                 )

    #                 log_fn(f"[NEW_FROM_USB] {init_hash}")
    #                 continue

    #             # =========================
    #             # CASO: existe en ambos
    #             # =========================
    #             same_hash = u["content_hash"] == local_row["content_hash"]
    #             same_path = u["rel_path"] == local_row["rel_path"]

    #             src = usb_path
    #             dst = self.pc_root / u["rel_path"]

    #             if same_hash and same_path:
    #                 continue

    #             if same_hash and not same_path:
    #                 old = self.pc_root / local_row["rel_path"]
    #                 move_file(old, dst)

    #                 loc_conn.execute(
    #                     "UPDATE files SET rel_path=? WHERE init_hash=?",
    #                     (u["rel_path"], init_hash),
    #                 )

    #                 log_fn(f"[MOVE_LOCAL] {init_hash}")
    #                 continue

    #             # contenido distinto → UPDATE
    #             copy_file(src, dst)
    #             new_hash = sha256_file(dst)

    #             if new_hash != u["content_hash"]:
    #                 raise RuntimeError(f"Hash mismatch (UPDATE): {init_hash}")

    #             if not same_path:
    #                 old = self.pc_root / local_row["rel_path"]
    #                 if old.exists():
    #                     old.unlink()

    #             loc_conn.execute(
    #                 """UPDATE files
    #                 SET content_hash=?, rel_path=?, timestamp=?
    #                 WHERE init_hash=?""",
    #                 (new_hash, u["rel_path"], u["timestamp"], init_hash),
    #             )

    #             log_fn(f"[UPDATE_LOCAL] {init_hash}")

    #     usb_conn.close()
    #     loc_conn.close()
