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
