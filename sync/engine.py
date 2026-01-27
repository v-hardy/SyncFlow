import socket
import time
import logging
from pathlib import Path

from sync.database import DB
from sync.domain import MovementRules, CurrentState
from sync.fs_util import FSOps
from sync.meta_util import walk_directory_metadata, sha256_file


MTIME_TOLERANCE = 2  # segundos


class EngineSync:
    # <======================================= INIT =======================================>
    def __init__(self, pc_root: Path, usb_root: Path, db_name: str):
        self.machine_name = socket.gethostname()
        self.pc_root = pc_root.resolve()
        self.usb_root = usb_root.resolve()
        self.db = DB(self.pc_root, self.usb_root, db_name)
        self.logger = logging.getLogger(__name__)

        self.logger.info(
            "EngineSync iniciado | machine=%s | pc_root=%s | usb_root=%s",
            self.machine_name,
            self.pc_root,
            self.usb_root,
        )

    # <======================================= FASE 1 =======================================>
    def replicate_master(self):
        self.logger.info("FASE 1: replicando estado desde USB")

        primary_master, primary_tombstones = self._read_usb_master()
        if not primary_master:
            self.logger.info("USB sin master_states, salto replicación")
            return

        secundary_master = self._read_pc_master()
        if not secundary_master:
            self.logger.warning("PC sin master_states, posible primera ejecución")
            self._initial_usb_copy(primary_master)
            return

        self._sync_usb_to_pc(primary_master, secundary_master, primary_tombstones)

    def _read_usb_master(self):
        with self.db.get_db_connection(self.db.usb_path) as conn:
            if self.db.table_is_empty(conn, "master_states"):
                return [], []
            return self.db.read_states(conn), self.db.read_tombstones(conn)

    def _read_pc_master(self):
        with self.db.get_db_connection(self.db.pc_path) as conn:
            if self.db.table_is_empty(conn, "master_states"):
                return []
            return self.db.read_states(conn)

    def _initial_usb_copy(self, usb_master):
        for entry in usb_master:
            src = self.usb_root / entry["rel_path"]
            dst = self.pc_root / entry["rel_path"]
            FSOps.copy_file(src, dst)

    def _sync_usb_to_pc(self, usb_master, pc_master, tombstones):
        pc_index = {m["init_hash"]: m for m in pc_master}
        usb_index = {m["init_hash"]: m for m in usb_master}
        tombstone_index = {m["init_hash"]: m for m in tombstones}

        for h in pc_index.keys() | usb_index.keys():
            pc = pc_index.get(h)
            usb = usb_index.get(h)

            if usb and not pc:
                self._copy_usb_to_pc(usb)
            elif pc and not usb:
                self._delete_pc_if_tombstone(pc, tombstone_index)
            else:
                self._resolve_conflict(pc, usb)

    def _copy_usb_to_pc(self, usb):
        self.logger.info("COPY USB → PC | %s", usb["rel_path"])
        src = self.usb_root / usb["rel_path"]
        dst = self.pc_root / usb["rel_path"]
        FSOps.copy_file(src, dst)

    def _delete_pc_if_tombstone(self, pc, tombstones):
        if pc["init_hash"] in tombstones:
            self.logger.info("DELETE en PC (tombstone) | %s", pc["rel_path"])
            FSOps.delete_file(self.pc_root / pc["rel_path"])

    def _resolve_conflict(self, pc, usb):
        src_usb = self.usb_root / usb["rel_path"]
        dst_pc = self.pc_root / usb["rel_path"]

        if usb["rel_path"] != pc["rel_path"] and usb["mtime"] == pc["mtime"]:
            self.logger.info(
                "MOVE detectado | %s → %s", pc["rel_path"], usb["rel_path"]
            )
            FSOps.move_file(self.pc_root / pc["rel_path"], dst_pc)
            return

        if usb["mtime"] > pc["mtime"] and usb["content_hash"] != pc["content_hash"]:
            self.logger.info("UPDATE desde USB | %s", usb["rel_path"])
            FSOps.copy_file(src_usb, dst_pc)

    # <======================================= FASE 2 =======================================>
    def get_movements(self):
        self.logger.info("FASE 2 | Escaneando filesystem para detectar cambios")

        directory_tree = walk_directory_metadata(self.pc_root)

        with self.db.get_db_connection(self.db.pc_path) as conn:
            if self.db.table_is_empty(conn, "master_states"):
                self.logger.warning("FASE 2 | No hay master_states")
                return
            master = self.db.read_states(conn)

        paths_index = {m["rel_path"]: m for m in master}
        hash_index = {m["content_hash"]: m for m in master}

        with self.db.get_db_connection(self.db.temp_path) as temp_conn:
            self._detect_fs_changes(directory_tree, paths_index, hash_index, temp_conn)
            self._detect_deletes(directory_tree, paths_index, temp_conn)

    def _detect_fs_changes(self, tree, paths_index, hash_index, conn):
        for rel_path, (size, mtime, _) in tree.items():
            db_entry = paths_index.get(rel_path)

            if not db_entry:
                self._handle_new_entry(rel_path, size, mtime, hash_index, conn)
            else:
                self._handle_existing_entry(rel_path, size, mtime, db_entry, conn)

    def _handle_new_entry(self, rel_path, size, mtime, hash_index, conn):
        current_hash = sha256_file(self.pc_root / rel_path)
        previous = hash_index.get(current_hash)

        if not previous:
            op = "CREATE"
            rel_old = None
            init_hash = current_hash
        else:
            op = "MOVE"
            rel_old = previous["rel_path"]
            init_hash = previous["init_hash"]

        self.logger.info("FASE 2 | %s detectado | %s", op, rel_path)

        self.db.upsert_movement(
            conn,
            {
                "op_type": op,
                "init_hash": init_hash,
                "rel_path": rel_old or rel_path,
                "new_rel_path": rel_path if op == "MOVE" else None,
                "content_hash": current_hash,
                "size_bytes": size,
                "last_op_time": mtime,
                "machine_name": self.machine_name,
            },
        )

    def _handle_existing_entry(self, rel_path, size, mtime, db_entry, conn):
        if (
            db_entry["size_bytes"] == size
            and abs(db_entry["last_op_time"] - mtime) <= MTIME_TOLERANCE
        ):
            return

        current_hash = sha256_file(self.pc_root / rel_path)
        if current_hash != db_entry["content_hash"]:
            self.logger.info("FASE 2 | MODIFY detectado | %s", rel_path)
            self.db.upsert_movement(
                conn,
                {
                    "op_type": "MODIFY",
                    "init_hash": db_entry["init_hash"],
                    "rel_path": rel_path,
                    "new_rel_path": None,
                    "content_hash": current_hash,
                    "size_bytes": size,
                    "last_op_time": mtime,
                    "machine_name": self.machine_name,
                },
            )

    def _detect_deletes(self, tree, paths_index, conn):
        for rel_path in paths_index.keys() - tree.keys():
            entry = paths_index[rel_path]
            self.logger.info("FASE 2 | DELETE detectado | %s", rel_path)

            self.db.upsert_movement(
                conn,
                {
                    "op_type": "DELETE",
                    "init_hash": entry["init_hash"],
                    "rel_path": rel_path,
                    "new_rel_path": None,
                    "content_hash": entry["content_hash"],
                    "size_bytes": entry["size_bytes"],
                    "last_op_time": time.time(),
                    "machine_name": self.machine_name,
                },
            )

    # <======================================= FASE 3 =======================================>
    def apply_movements(self):
        self.logger.info("FASE 3 | Aplicando movimientos y sincronizando USB")

        with self.db.get_db_connection(self.db.temp_path) as conn:
            if self.db.table_is_empty(conn, "movements"):
                self.logger.info("FASE 3 | No hay movimientos pendientes")
                return

            movements = self.db.read_movements(conn)
            master = self.db.read_states(conn)
            current = CurrentState({m["rel_path"] for m in master})

            for mov in movements:
                self._apply_single_movement(mov, current, conn)

    def _apply_single_movement(self, mov, current, conn):
        if not MovementRules.can_apply(mov, current._paths):
            self.logger.warning(
                "FASE 3 | Movimiento omitido | op=%s path=%s",
                mov["op_type"],
                mov["rel_path"],
            )
            return

        try:
            self._apply_fs_operation(mov)
            self.db.update_state(conn, mov)
            self.db.archive_and_delete_movement(conn, mov)

            self.logger.info(
                "FASE 3 | Movimiento aplicado | op=%s path=%s",
                mov["op_type"],
                mov["rel_path"],
            )
        except Exception as e:
            self.logger.error("Error aplicando movimiento %s: %s", mov, e)

    def _apply_fs_operation(self, mov):
        src = self.pc_root / mov["rel_path"]
        dst = self.usb_root / mov["rel_path"]

        if mov["op_type"] in {"CREATE", "MODIFY"}:
            FSOps.copy_file(src, dst)
        elif mov["op_type"] == "MOVE":
            FSOps.move_file(src, self.usb_root / mov["new_rel_path"])
        elif mov["op_type"] == "DELETE":
            FSOps.delete_file(dst)
        else:
            raise ValueError(f"Operación desconocida: {mov['op_type']}")
