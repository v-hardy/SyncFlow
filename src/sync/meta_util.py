import hashlib
import logging
from pathlib import Path

logger = logging.getLogger("fs.scan")


# <======================================= OBTENER HASH DEL ARCHIVO =======================================>
def sha256_file(path: Path, chunk_size: int = 8192) -> str:
    logger.debug("HASH_START | %s", path)
    h = hashlib.sha256()
    try:
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                h.update(chunk)
        digest = h.hexdigest()
        logger.debug("HASH_DONE | %s | %s", path, digest)
        return digest
    except Exception:
        logger.exception("Error calculando hash: %s", path)
        raise


# <======================================= GENERAR DICCIONARIO CON METADATOS =======================================>
def walk_directory_metadata(root: Path) -> dict[str, tuple[int, float, str | None]]:
    """
    Devuelve dict:
    CLAVE: rel_path -> VALOR: (size, mtime, hash_or_none)
    hash_or_none: solo si es necesario más tarde. Por defecto: None
    """
    logger.info("SCAN_START | root=%s", root)

    snapshot = {}
    files = 0
    try:
        for file_path in root.rglob("*"):
            # .rglob("*") es un método recursivo que busca todos los elementos (archivos y subdirectorios). Es equivalente a glob("**/*", recursive=True), pero usando el estilo de pathlib.
            if file_path.is_file():
                # .is_file(): Filtra solo los archivos reales (excluye directorios, enlaces simbólicos que apunten a directorios, etc.). Ignora carpetas vacías o subdirectorios.
                rel_path = str(
                    file_path.relative_to(root).as_posix()
                )  # .relative_to(root) → devuelve un Path relativo (ej. sub/carpeta/archivo.txt). y .as_posix() → convierte a string usando barras / (formato POSIX), incluso en Windows. Esto asegura que el snapshot sea portable entre sistemas operativos.
                stat = file_path.stat()  # .stat obtiene metadatos del archivo
                snapshot[rel_path] = (
                    stat.st_size,
                    stat.st_mtime,
                    None,
                )  # hash calculado bajo demanda

                files += 1
    except Exception:
        logger.exception("Error escaneando directorio: %s", root)
        raise

    logger.info("SCAN_DONE | root=%s | files=%d", root, files)

    return snapshot
