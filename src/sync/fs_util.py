import shutil
import logging
from pathlib import Path

"""
El FS no decide nada, solo ejecuta.
Operaciones básicas:
- Crear archivo
- Escribir contenido
- Mover archivo
- Borrar archivo
- Crear carpetas si no existen
"""

logger = logging.getLogger("fs")


class FSOps:
    @staticmethod
    def ensure_parent(path: Path):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            logger.debug("ensure_parent | %s", path.parent)
        except Exception:
            logger.exception("Error creando carpeta padre: %s", path.parent)
            raise

    @staticmethod
    def create_file(path: Path, content: bytes):
        logger.debug("CREATE_FILE | %s (%d bytes)", path, len(content))
        try:
            FSOps.ensure_parent(path)
            with open(path, "wb") as f:
                f.write(content)
        except Exception:
            logger.exception("Error creando archivo: %s", path)
            raise

    @staticmethod
    def modify_file(path: Path, content: bytes):
        logger.debug("MODIFY_FILE | %s (%d bytes)", path, len(content))
        try:
            with open(path, "wb") as f:
                f.write(content)
        except Exception:
            logger.exception("Error modificando archivo: %s", path)
            raise

    @staticmethod
    def copy_file(src: Path, dst: Path):
        logger.debug("COPY_FILE | %s → %s", src, dst)
        try:
            FSOps.ensure_parent(dst)
            shutil.copy2(src, dst)
        except Exception:
            logger.exception("Error copiando archivo: %s → %s", src, dst)
            raise

    @staticmethod
    def move_file(src: Path, dst: Path):
        logger.debug("MOVE_FILE | %s → %s", src, dst)
        try:
            FSOps.ensure_parent(dst)
            src.rename(dst)
        except Exception:
            logger.exception("Error moviendo archivo: %s → %s", src, dst)
            raise

    @staticmethod
    def delete_file(path: Path):
        if not path.exists():
            logger.debug("DELETE_FILE ignorado (no existe): %s", path)
            return

        logger.debug("DELETE_FILE | %s", path)
        try:
            path.unlink()
        except Exception:
            logger.exception("Error borrando archivo: %s", path)
            raise
