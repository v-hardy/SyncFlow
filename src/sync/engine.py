import socket
import time
from pathlib import Path

import logging
from database import DB
from domain import MovementRules, CurrentState
from fs_util import FSOps
from meta_util import walk_directory_metadata, sha256_file


class EngineSync:
    # <======================================= INICIAR OBJETO =======================================>
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

    # <======================================= FASE 1: SINC DESDE USB =======================================>
    def replicateMaster(self, log_fn=print):
        self.logger.info("FASE 1: replicando estado desde USB")

        with self.db.get_db_connection(self.db.usb_path) as usb_conn:
            if self.db.table_is_empty(usb_conn, "master_states"):
                return
            primary_master = self.db.read_states(usb_conn)
            primary_tombstones = self.db.read_tombstones(usb_conn)

        with self.db.get_db_connection(self.db.pc_path) as pc_conn:
            if self.db.table_is_empty(pc_conn, "master_states"):
                return
            secundary_master = self.db.read_states(pc_conn)

        if primary_master:
            if secundary_master:
                pc_index = {m["init_hash"]: m for m in secundary_master}
                usb_index = {m["init_hash"]: m for m in primary_master}
                tombstones_index = {m["init_hash"]: m for m in primary_tombstones}

                pc_hashes = set(pc_index)
                usb_hashes = set(usb_index)

                all_hashes = pc_hashes | usb_hashes  # (- .db)'s ?

                for h in all_hashes:
                    self.logger.debug("Evaluando hash=%s", h)

                    pc = pc_index.get(h)
                    usb = usb_index.get(h)

                    src = self.usb_root / usb["rel_path"]
                    dst = self.pc_root / src.relative_to(self.usb_root)

                    # 1 Está en USB y NO en PC → copiar a PC
                    if usb and not pc:
                        self.logger.info("COPY USB → PC | %s", usb["rel_path"])
                        FSOps.copy_file(src, dst)
                        continue

                    # 2 Está en PC y NO en USB → borrar en PC
                    if pc and not usb:
                        if tombstones_index.get(h):
                            self.logger.info(
                                "DELETE en PC (tombstone) | %s", pc["rel_path"]
                            )
                            dst = self.pc_root / pc["rel_path"]
                            FSOps.delete_file(dst)
                        continue

                    # 3 Está en ambos → comparar
                    if usb and pc:
                        # 3.1 Movido (mismo hash, distinto path, mismo contenido)
                        if (
                            usb["rel_path"] != pc["rel_path"]
                            and usb["mtime"] == pc["mtime"]
                        ):
                            self.logger.info(
                                "MOVE detectado | %s → %s",
                                pc["rel_path"],
                                usb["rel_path"],
                            )
                            src = self.pc_root / pc["rel_path"]
                            FSOps.move_file(src, dst)
                            continue

                        # 3.2 USB más reciente → copiar
                        if (
                            usb["mtime"] > pc["mtime"]
                            and usb["content_hash"] != pc["content_hash"]
                        ):
                            self.logger.info("UPDATE desde USB | %s", usb["rel_path"])
                            FSOps.copy_file(src, dst)
                            continue

                        # 3.3 Iguales o es un NUEVO archivo solo en pc → no hacer nada

            else:
                self.logger.warning("PC sin master_states, posible primera ejecución")
                for usb_file in primary_master:
                    src = self.usb_root / usb_file["rel_path"]
                    dst = self.pc_root / src.relative_to(self.usb_root)
                    FSOps.copy_file(src, dst)
        else:
            self.logger.info("USB sin master_states, salto replicación")

    # <======================================= FASE 2: OBTENER DATOS DE CAMBIOS =======================================>
    def get_movements(self, log_fn: print):
        """
        Compara el estado actual del filesystem con el estado guardado en la DB
        y registra los movimientos (CREATE / MODIFY / MOVE / DELETE).
        """

        directory_tree = walk_directory_metadata(self.pc_root)

        with self.db.get_db_connection(self.db.pc_path) as pc_conn:
            if self.db.table_is_empty(pc_conn, "master_states"):
                return
            secundary_master = self.db.read_states(pc_conn)

        master_paths_index = {m["rel_path"]: m for m in secundary_master}
        master_hashs_index = {m["content_hash"]: m for m in secundary_master}

        with self.db.get_db_connection(self.db.temp_path) as temp_conn:
            for rel_path, (size, mtime, hash_or_none) in directory_tree.items():
                print(f"Archivo: {rel_path}")
                print(f"  Tamaño: {size} bytes")
                print(f"  Modificado: {mtime}")
                print(f"  Hash: {hash_or_none}")
                print("-" * 20)

                # Trato todo el contenido FS
                db_entry = master_paths_index.get(rel_path)
                if db_entry is None:
                    # NUEVA ENTRADA
                    current_hash = sha256_file(self.pc_root / rel_path)
                    db_entry = master_hashs_index.get(current_hash)
                    if db_entry is None:
                        novedad = {
                            "op_type": "CREATE",
                            "init_hash": current_hash,
                            "rel_path": rel_path,
                            "new_rel_path": None,
                            "content_hash": current_hash,
                            "size_bytes": size,
                            "last_op_time": mtime,
                            "machine_name": self.machine_name,
                        }
                        self.db.upsert_movement(temp_conn, novedad, log_fn=print)
                        log_fn(f"ADD MOV -> CREATE: {rel_path}")
                    else:
                        # Movido sin ser modificado
                        novedad = {
                            "op_type": "MOVE",
                            "init_hash": db_entry["init_hash"],
                            "rel_path": db_entry["rel_path"],
                            "new_rel_path": rel_path,
                            "content_hash": current_hash,
                            "size_bytes": size,
                            "last_op_time": mtime,
                            "machine_name": self.machine_name,
                        }
                        self.db.upsert_movement(temp_conn, novedad, log_fn=print)
                        log_fn(f"ADD MOV -> MOVE TO: {rel_path}")

                else:
                    # LA ENTRADA YA EXISTE
                    MTIME_TOLERANCE = 2  # segundos
                    if (
                        db_entry["size_bytes"] == size
                        and abs(db_entry["last_op_time"] - mtime) <= MTIME_TOLERANCE
                    ):
                        pass
                        # asumir que no cambió
                    else:
                        current_hash = sha256_file(self.pc_root / rel_path)
                        if db_entry["content_hash"] != current_hash:
                            novedad = {
                                "op_type": "MODIFY",
                                "init_hash": db_entry["init_hash"],
                                "rel_path": rel_path,
                                "new_rel_path": None,
                                "content_hash": current_hash,
                                "size_bytes": size,
                                "last_op_time": mtime,
                                "machine_name": self.machine_name,
                            }
                            self.db.upsert_movement(temp_conn, novedad, log_fn=print)
                            log_fn(f"ADD MOV -> MODIFY: {rel_path}")

            # Archivos eliminados: existen en DB pero no en filesystem
            deleted_paths = master_paths_index.keys() - directory_tree.keys()

            for path in deleted_paths:
                db_entry = master_paths_index.get(path)
                novedad = {
                    "op_type": "DELETE",
                    "init_hash": db_entry["init_hash"],
                    "rel_path": db_entry["rel_path"],
                    "new_rel_path": None,
                    "content_hash": db_entry["content_hash"],
                    "size_bytes": db_entry["size_bytes"],
                    "last_op_time": time.time(),
                    "machine_name": self.machine_name,
                }
                self.db.upsert_movement(temp_conn, novedad, log_fn=print)
                log_fn(f"ADD MOV -> DELETE: {path}")

    # <======================================= FASE 3: APLICAR CAMBIOS Y SYNC =======================================>
    def apply_movements(self):
        # trabajo sobre una db temp (copia de db local)
        with self.db.get_db_connection(self.db.temp_path) as temp_conn:
            if self.db.table_is_empty(temp_conn, "movements"):
                return
            movements = self.db.read_movements(temp_conn)

            master = self.db.read_states(temp_conn)
            master_paths = {m["rel_path"] for m in master}
            current = CurrentState(master_paths)

            for mov in movements:
                if not MovementRules.can_apply(mov, current._paths):
                    continue  # Omito lo que sigue por conflicto en reglas

                src = self.pc_root / mov["rel_path"]
                dst = self.usb_root / src.relative_to(self.pc_root)

                # APLICAR NOVEDAD FS
                op = mov["op_type"]

                if op == "CREATE":
                    # FSOps.copy_file(src, dst)
                    # Validar .sha256_file en Destino, Si IGUALES -> VALIDADO
                    try:
                        FSOps.copy_file(src, dst)
                    except FileNotFoundError:
                        self.logger.warning(f"No se pudo copiar (no existe): {src}")
                    except Exception as e:
                        self.logger.error(f"Error copiando {src}: {e}")

                elif op == "MODIFY":
                    FSOps.copy_file(src, dst)

                elif op == "MOVE":
                    dst = self.usb_root / mov["new_rel_path"]
                    FSOps.move_file(src, dst)
                    # Validar .exist en new_rel, Si EXISTE -> VALIDADO

                elif op == "DELETE":
                    FSOps.delete_file(dst)
                    # Validar NO(.exist) en rel_path -> Si NO EXISTE -> VALIDADO

                else:
                    raise ValueError(f"Operación desconocida: {op}")
                # if VALIDADO:
                # APLICAR NOVEDADES DB
                self.db.update_state(temp_conn, mov)
                self.db.archive_and_delete_movement(temp_conn, mov)
            # lista en memoria vacia significa que no hubo errores
