"""
SQL debe encargarse de:
CREATE / MODIFY / MOVE / DELETE lógicos
Orden de aplicación (op_time)
Evitar duplicados
Idempotencia
Saber qué hay que hacer, no cómo
"""

import sqlite3
from pathlib import Path
import time


class DB:
    def __init__(self, pc_root: Path, usb_root: Path, db_name: str):
        self.pc_root = pc_root.resolve()
        self.usb_root = usb_root.resolve()
        self.pc_path = self.pc_root / ".sync" / db_name
        self.temp_path = self.pc_root / ".sync" / f"{db_name}.tmp"
        self.usb_path = self.usb_root / db_name

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
                size_bytes,
                last_op_time,
                machine_name
            FROM master_states
            ORDER BY rel_path ASC
            """
        )

        master_states = [dict(row) for row in cursor.fetchall()]

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
                size_bytes,
                last_op_time,
                machine_name
            FROM movements
            ORDER BY rel_path ASC
            """
        )

        movements = [dict(row) for row in cursor.fetchall()]

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

        tombstones = [dict(row) for row in cursor.fetchall()]

        return tombstones

    # <======================================= ACTUALIZA =======================================>
    def update_state(self, conn, mov: dict):
        op = mov["op_type"]

        if op == "CREATE":
            conn.execute(
                """
                INSERT INTO master_states (
                    init_hash,
                    rel_path,
                    content_hash,
                    size_bytes,
                    last_op_time,
                    machine_name
                ) 
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    mov["init_hash"],
                    mov["rel_path"],
                    mov["content_hash"],
                    mov["size_bytes"],
                    mov["last_op_time"],
                    mov["machine_name"],
                ),
            )

        elif op == "MODIFY":
            conn.execute(
                """
                UPDATE master_states
                SET
                    content_hash   = ?,
                    size_bytes     = ?,
                    last_op_time   = ?,
                    last_machine   = ?
                WHERE rel_path = ?
                """,
                (
                    mov["content_hash"],
                    mov["size_bytes"],
                    mov["last_op_time"],
                    mov["machine_name"],
                    mov["rel_path"],
                ),
            )

        elif op == "MOVE":
            conn.execute(
                """
                UPDATE master_states
                SET rel_path = ?
                WHERE rel_path = ?
                """,
                (
                    mov["new_rel_path"],
                    mov["rel_path"],
                ),
            )

        elif op == "DELETE":
            conn.execute(
                """
                DELETE FROM master_states 
                WHERE rel_path = ?
                """,
                (mov["rel_path"],),
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
                    mov["last_op_time"],
                    mov["machine_name"],
                ),
            )

    def upsert_movements(self, conn, mov: dict, log_fn=print):
        conn.execute(
            """
            INSERT OR REPLACE INTO movements (
                id, 
                op_type, 
                init_hash, 
                rel_path, n
                ew_rel_path,
                content_hash, 
                size_bytes,
                last_op_time, 
                machine_name, 
                applied_time
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                mov["id"],
                mov["op_type"],
                mov["init_hash"],
                mov["rel_path"],
                mov["new_rel_path"],
                mov["content_hash"],
                mov["size_bytes"],
                mov["last_op_time"],
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
                id, 
                op_type, 
                init_hash, 
                rel_path, 
                new_rel_path,
                content_hash, 
                size_bytes,
                last_op_time, 
                machine_name, 
                applied_time
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                mov["id"],
                mov["op_type"],
                mov["init_hash"],
                mov["rel_path"],
                mov["new_rel_path"],
                mov["content_hash"],
                mov["size_bytes"],
                mov["last_op_time"],
                mov["machine_name"],
                int(time.time()),
            ),
        )

        conn.execute("DELETE FROM movements WHERE id = ?", (mov["id"],))
