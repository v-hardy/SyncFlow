import shutil
from pathlib import Path

""" 
El FS no decide nada, solo ejecuta.
Operaciones básicas:
-Crear archivo
-Escribir contenido
-Mover archivo
-Borrar archivo
-Crear carpetas si no existen 
"""


class FSOps:
    @staticmethod
    def ensure_parent(path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def create_file(root: Path, rel_path: str, content: bytes):
        path = root / rel_path
        FSOps.ensure_parent(path)
        with open(path, "wb") as f:
            f.write(content)

    @staticmethod
    def copy_file(src_root: Path, dst_root: Path, rel_path: str):
        src = src_root / rel_path
        dst = dst_root / rel_path
        FSOps.ensure_parent(dst)
        shutil.copy2(src, dst)

    @staticmethod
    def move_file(root: Path, src_rel_path: str, dst_rel_path: str):
        src = root / src_rel_path
        dst = root / dst_rel_path
        FSOps.ensure_parent(dst)
        src.rename(dst)

    @staticmethod
    def modify_file(root: Path, rel_path: str, content: bytes):
        path = root / rel_path
        with open(path, "wb") as f:
            f.write(content)

    @staticmethod
    def delete_file(root: Path, rel_path: str):
        path = root / rel_path
        if path.exists():
            path.unlink()
