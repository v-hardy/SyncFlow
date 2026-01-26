#!/usr/bin/env python3

import sys
import argparse
import traceback
from pathlib import Path
import logging

from engine import EngineSync
from dry_run import dry_run

logger = logging.getLogger(__name__)


# =========================
# Logging
# =========================
def setup_logging(log_file: Path):
    # Asegurar que exista la carpeta del log
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
        force=True,  # evita configuraciones previas silenciosas
    )


# =========================
# Sanity checks
# =========================
def check_environment(pc_root, usb_root):
    logger.debug("Chequeando entorno: pc_root=%s, usb_root=%s", pc_root, usb_root)

    if not pc_root.exists():
        logger.error("PC root no existe: %s", pc_root)
        raise RuntimeError(f"PC root no existe: {pc_root}")

    if not usb_root.exists():
        logger.error("USB root no existe: %s", usb_root)
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

    logger.info("===== INICIO SYNC =====")

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
            logger.info("Ejecutando DRY-RUN")
            dry_run()
            logger.info("DRY-RUN finalizado. No se aplicaron cambios.")
            return

        # -------------------------
        # FASE 1
        # -------------------------
        logger.info("FASE 1: Sync desde USB → Local")
        engine.replicate_master()

        # -------------------------
        # FASE 2
        # -------------------------
        logger.info("FASE 2: Obteniendo movimientos locales")
        engine.get_movements()

        # -------------------------
        # FASE 3
        # -------------------------
        logger.info("FASE 3: Aplicando movimientos locales → USB")
        engine.apply_movements()

        logger.info("===== SYNC FINALIZADA OK =====")

    except Exception as e:
        logging.error("SYNC FALLIDA")
        logging.error(str(e))
        logging.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
