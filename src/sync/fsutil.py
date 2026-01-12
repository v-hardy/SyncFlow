import shutil
from pathlib import Path


def ensure_parent(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)


def copy_file(src: Path, dst: Path):
    ensure_parent(dst)
    shutil.copy2(src, dst)


def move_file(src: Path, dst: Path):
    ensure_parent(dst)
    src.replace(dst)


# <======================================= GENERAR DICCIONARIO CON METADATOS =======================================>
def walk_directory_metadata(root: Path) -> dict[str, tuple[int, float, str | None]]:
    """
    Devuelve dict:
    CLAVE: rel_path -> VALOR: (size, mtime, hash_or_none)
    hash_or_none: solo si es necesario más tarde. Por defecto: None
    """
    snapshot = {}
    for file_path in root.rglob(
        "*"
    ):  # .rglob("*") es un método recursivo que busca todos los elementos (archivos y subdirectorios). Es equivalente a glob("**/*", recursive=True), pero usando el estilo de pathlib.
        if file_path.is_file():  # .is_file(): Filtra solo los archivos reales (excluye directorios, enlaces simbólicos que apunten a directorios, etc.). Ignora carpetas vacías o subdirectorios.
            rel_path = str(
                file_path.relative_to(root).as_posix()
            )  # .relative_to(root) → devuelve un Path relativo (ej. sub/carpeta/archivo.txt). y .as_posix() → convierte a string usando barras / (formato POSIX), incluso en Windows. Esto asegura que el snapshot sea portable entre sistemas operativos.
            stat = file_path.stat()  # .stat obtiene metadatos del archivo
            snapshot[rel_path] = (
                stat.st_size,
                stat.st_mtime,
                None,
            )  # hash calculado bajo demanda
    return snapshot
