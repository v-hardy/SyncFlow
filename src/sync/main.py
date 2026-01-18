#!/usr/bin/env python3

import sys
import argparse
import traceback
from pathlib import Path
import logging

from engine import EngineSync
from dry_run import dry_run


# =========================
# Logging
# =========================
def setup_logging(log_file: Path):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


# =========================
# Sanity checks
# =========================
def check_environment(pc_root, usb_root):
    if not pc_root.exists():
        raise RuntimeError(f"PC root no existe: {pc_root}")

    if not usb_root.exists():
        raise RuntimeError(f"USB root no existe: {usb_root}")


# =========================
# MAIN
# =========================
def main():
    parser = argparse.ArgumentParser(
        description="Sincronización offline USB ↔ Local (maestro + delta)"
    )

    parser.add_argument(
        "--pc-root",
        required=True,
        type=Path,
        help="Directorio raíz local",
    )
    parser.add_argument(
        "--usb-root",
        required=True,
        type=Path,
        help="Directorio raíz de archivos en el USB",
    )
    parser.add_argument(
        "--db-name",
        default=str("metadata.db"),
        type=str,
        help="Nombre DB SQL Maestro",
    )
    parser.add_argument(
        "--log",
        default=Path("sync.log"),
        type=Path,
        help="Archivo de log",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simula el sync sin aplicar cambios",
    )

    args = parser.parse_args()

    setup_logging(args.log)

    logging.info("===== INICIO SYNC =====")

    try:
        check_environment(
            args.pc_root,
            args.usb_root,
        )

        # Instancia de motor
        engine = EngineSync(
            pc_root=args.pc_root,
            usb_root=args.usb_root,
            db_name=args.db_name,
        )

        # ==================================================
        # DRY-RUN
        # ==================================================
        if args.dry_run:
            logging.info("Ejecutando DRY-RUN")
            dry_run(log_fn=logging.info)
            logging.info("DRY-RUN finalizado. No se aplicaron cambios.")
            return

        # -------------------------
        # FASE 1
        # -------------------------
        logging.info("FASE 1: Sync desde USB → Local")
        engine.replicateMaster(log_fn=logging.info)

        # -------------------------
        # FASE 2
        # -------------------------
        logging.info("FASE 2: Obteniendo movimientos locales")
        engine.get_movements(log_fn=logging.info)

        # -------------------------
        # FASE 3
        # -------------------------
        logging.info("FASE 3: Aplicando movimientos locales → USB")
        engine.apply_movements(log_fn=logging.info)

        logging.info("===== SYNC FINALIZADA OK =====")

    except Exception as e:
        logging.error("SYNC FALLIDA")
        logging.error(str(e))
        logging.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
