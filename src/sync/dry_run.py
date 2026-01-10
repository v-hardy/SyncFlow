import sqlite3
from pathlib import Path
from collections import Counter


def dry_run(
    usb_root: Path, local_root: Path, usb_db: Path, local_db: Path, log_fn=print
):
    usb_conn = sqlite3.connect(usb_db)
    loc_conn = sqlite3.connect(local_db)

    usb_conn.row_factory = sqlite3.Row
    loc_conn.row_factory = sqlite3.Row

    stats = Counter()

    log_fn("===== DRY RUN =====")

    # =========================
    # FASE 1 (USB → LOCAL)
    # =========================
    log_fn("DRY-RUN FASE 1: USB → LOCAL")

    usb_rows = usb_conn.execute("SELECT init_hash, content_hash, rel_path FROM files")

    for u in usb_rows:
        local_row = loc_conn.execute(
            "SELECT content_hash, rel_path FROM files WHERE init_hash=?",
            (u["init_hash"],),
        ).fetchone()

        if local_row is None:
            log_fn(f"[WOULD COPY NEW FROM USB] {u['rel_path']}")
            stats["new_from_usb"] += 1
            continue

        same_hash = u["content_hash"] == local_row["content_hash"]
        same_path = u["rel_path"] == local_row["rel_path"]

        if same_hash and same_path:
            continue

        if same_hash and not same_path:
            log_fn(f"[WOULD MOVE LOCAL] {local_row['rel_path']} → {u['rel_path']}")
            stats["move_local"] += 1
            continue

        if not same_hash:
            log_fn(f"[WOULD UPDATE LOCAL] {u['rel_path']}")
            stats["update_local"] += 1

    # =========================
    # FASE 3 (LOCAL → USB)
    # =========================
    log_fn("DRY-RUN FASE 3: LOCAL → USB")

    movements = loc_conn.execute("SELECT * FROM movements ORDER BY id")

    for m in movements:
        op = m["op_type"]

        if op == "CREATE":
            log_fn(f"[WOULD CREATE ON USB] {m['new_rel_path']}")
            stats["create_usb"] += 1

        elif op == "MODIFY":
            log_fn(f"[WOULD MODIFY ON USB] {m['new_rel_path']}")
            stats["modify_usb"] += 1

        elif op == "MOVE":
            log_fn(f"[WOULD MOVE ON USB] {m['old_rel_path']} → {m['new_rel_path']}")
            stats["move_usb"] += 1

        elif op == "DELETE":
            log_fn(f"[WOULD DELETE ON USB] {m['init_hash']}")
            stats["delete_usb"] += 1

    # =========================
    # RESUMEN
    # =========================
    log_fn("===== DRY RUN RESUMEN =====")
    for k, v in stats.items():
        log_fn(f"{k}: {v}")

    usb_conn.close()
    loc_conn.close()

    return stats
