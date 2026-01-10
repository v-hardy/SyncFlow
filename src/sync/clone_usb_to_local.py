import shutil
from pathlib import Path


def clone_usb_to_local(usb_root: Path, local_root: Path, usb_db: Path, local_db: Path):
    if local_root.exists():
        shutil.rmtree(local_root)

    shutil.copytree(usb_root, local_root)
    shutil.copy2(usb_db, local_db)
