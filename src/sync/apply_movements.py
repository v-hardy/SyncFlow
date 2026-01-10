import sqlite3
import socket
from pathlib import Path

from hashutil import sha256_file
from fsutil import copy_file

MACHINE = socket.gethostname()


def apply_movements(
    usb_root: Path, usb_db: Path, local_root: Path, local_db: Path, log_fn=print
):
    usb_conn = sqlite3.connect(usb_db)
    loc_conn = sqlite3.connect(local_db)

    usb_conn.row_factory = sqlite3.Row
    loc_conn.row_factory = sqlite3.Row

    with usb_conn:
        usb_conn.execute("BEGIN IMMEDIATE")

        movements = loc_conn.execute(
            """SELECT * FROM movements ORDER BY id"""
        ).fetchall()

        for m in movements:
            op = m["op_type"]
            init_hash = m["init_hash"]
            op_time = m["op_time"]

            usb_row = usb_conn.execute(
                "SELECT * FROM files WHERE init_hash=?", (init_hash,)
            ).fetchone()

            # =========================
            # CREATE
            # =========================
            if op == "CREATE":
                if usb_row:
                    continue  # ya existe, no duplicar

                src = local_root / m["new_rel_path"]
                dst = usb_root / m["new_rel_path"]

                copy_file(src, dst)
                h = sha256_file(dst)

                usb_conn.execute(
                    "INSERT INTO files VALUES (?,?,?,?)",
                    (init_hash, h, m["new_rel_path"], op_time),
                )

                log_fn(f"[USB CREATE] {init_hash}")
