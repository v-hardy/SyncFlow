import logging
from pathlib import Path
from collections import Counter
from sync.engine import EngineSync
from sync.database import DB


def dry_run(engine: EngineSync, log_fn=logging.info):
    """
    Simula el proceso de sincronización sin aplicar cambios reales.
    Usa el engine existente para detectar cambios sin ejecutar operaciones FS.
    """
    stats = Counter()

    log_fn("===== DRY RUN =====")

    # =========================
    # FASE 1 (USB → LOCAL)
    # =========================
    log_fn("DRY-RUN FASE 1: USB → LOCAL")

    try:
        primary_master, primary_tombstones = engine._read_usb_master()
        if not primary_master:
            log_fn("USB sin master_states - se iniciaría desde cero")
        else:
            secundary_master = engine._read_pc_master()
            if not secundary_master:
                log_fn("PC sin master_states - se copiaría todo desde USB")
                stats["initial_copy"] = len(primary_master)
            else:
                pc_index = {m["init_hash"]: m for m in secundary_master}
                usb_index = {m["init_hash"]: m for m in primary_master}
                tombstone_index = {m["init_hash"]: m for m in primary_tombstones}

                for h in pc_index.keys() | usb_index.keys():
                    pc = pc_index.get(h)
                    usb = usb_index.get(h)

                    if usb and not pc:
                        log_fn(f"[WOULD COPY NEW FROM USB] {usb['rel_path']}")
                        stats["new_from_usb"] += 1
                    elif pc and not usb:
                        if pc["init_hash"] in tombstone_index:
                            log_fn(f"[WOULD DELETE FROM PC (tombstone)] {pc['rel_path']}")
                            stats["delete_pc_tombstone"] += 1
                    else:
                        # Conflicto o cambio
                        if usb["rel_path"] != pc["rel_path"]:
                            log_fn(f"[WOULD MOVE LOCAL] {pc['rel_path']} → {usb['rel_path']}")
                            stats["move_local"] += 1
                        elif usb["content_hash"] != pc["content_hash"]:
                            log_fn(f"[WOULD UPDATE LOCAL] {usb['rel_path']}")
                            stats["update_local"] += 1
    except Exception as e:
        log_fn(f"Error en FASE 1: {e}")

    # =========================
    # FASE 2 (DETECTAR CAMBIOS LOCALES)
    # =========================
    log_fn("DRY-RUN FASE 2: DETECTAR CAMBIOS LOCALES")

    try:
        from sync.meta_util import walk_directory_metadata

        directory_tree = walk_directory_metadata(engine.pc_root)

        with engine.db.get_db_connection(engine.db.pc_path) as conn:
            if engine.db.table_is_empty(conn, "master_states"):
                log_fn("No hay master_states - se escanearía todo como nuevo")
            else:
                master = engine.db.read_states(conn)
                paths_index = {m["rel_path"]: m for m in master}
                hash_index = {m["content_hash"]: m for m in master}

                # Detectar nuevos y modificados
                for rel_path, (size, mtime, _) in directory_tree.items():
                    db_entry = paths_index.get(rel_path)

                    if not db_entry:
                        log_fn(f"[WOULD DETECT CREATE] {rel_path}")
                        stats["create_local"] += 1
                    else:
                        if (
                            db_entry["size_bytes"] != size
                            or abs(db_entry["last_op_time"] - mtime) > 2
                        ):
                            log_fn(f"[WOULD DETECT MODIFY] {rel_path}")
                            stats["modify_local"] += 1

                # Detectar borrados
                for rel_path in paths_index.keys() - directory_tree.keys():
                    log_fn(f"[WOULD DETECT DELETE] {rel_path}")
                    stats["delete_local"] += 1
    except Exception as e:
        log_fn(f"Error en FASE 2: {e}")

    # =========================
    # FASE 3 (LOCAL → USB)
    # =========================
    log_fn("DRY-RUN FASE 3: LOCAL → USB")

    try:
        with engine.db.get_db_connection(engine.db.temp_path) as conn:
            if engine.db.table_is_empty(conn, "movements"):
                log_fn("No hay movimientos pendientes")
            else:
                movements = engine.db.read_movements(conn)
                master = engine.db.read_states(conn)
                from sync.domain import CurrentState
                current = CurrentState({m["rel_path"] for m in master})

                for mov in movements:
                    op = mov["op_type"]

                    from sync.domain import MovementRules
                    if not MovementRules.can_apply(mov, current._paths):
                        log_fn(f"[WOULD SKIP] {op} {mov['rel_path']} (no se puede aplicar)")
                        stats["skipped"] += 1
                        continue

                    if op == "CREATE":
                        log_fn(f"[WOULD CREATE ON USB] {mov['rel_path']}")
                        stats["create_usb"] += 1
                    elif op == "MODIFY":
                        log_fn(f"[WOULD MODIFY ON USB] {mov['rel_path']}")
                        stats["modify_usb"] += 1
                    elif op == "MOVE":
                        log_fn(f"[WOULD MOVE ON USB] {mov['rel_path']} → {mov['new_rel_path']}")
                        stats["move_usb"] += 1
                    elif op == "DELETE":
                        log_fn(f"[WOULD DELETE ON USB] {mov['rel_path']}")
                        stats["delete_usb"] += 1
    except Exception as e:
        log_fn(f"Error en FASE 3: {e}")

    # =========================
    # RESUMEN
    # =========================
    log_fn("===== DRY RUN RESUMEN =====")
    for k, v in stats.items():
        log_fn(f"{k}: {v}")

    return stats
