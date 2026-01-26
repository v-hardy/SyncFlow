#!/usr/bin/env python3
"""
Lanzador multiplataforma para el script de sync.
Este script permite ejecutar main.py con tus argumentos actuales de manera segura en cualquier sistema operativo.
"""

import sys
import subprocess
from pathlib import Path
import argparse

# ====================================
# Parser de argumentos del lanzador
# ====================================
parser = argparse.ArgumentParser(description="Lanzador multiplataforma para sync")

parser.add_argument("--pc-root", required=True, type=Path, help="Directorio raíz local")
parser.add_argument(
    "--usb-root", required=True, type=Path, help="Directorio raíz de archivos en el USB"
)
parser.add_argument(
    "--db-name", default="metadata.db", type=str, help="Nombre DB SQL Maestro"
)
parser.add_argument("--log", default=Path("sync.log"), type=Path, help="Archivo de log")
parser.add_argument(
    "--dry-run", action="store_true", help="Simula el sync sin aplicar cambios"
)

# Captura cualquier argumento extra para pasarlo a main.py
args, extra = parser.parse_known_args()

# ====================================
# Construir comando para main.py
# ====================================
python_cmd = sys.executable  # usa el mismo intérprete que este script

main_py_path = Path(__file__).parent / "sync" / "main.py"
if not main_py_path.exists():
    print(f"Error: no se encontró {main_py_path}")
    sys.exit(1)

cmd = [
    python_cmd,
    str(main_py_path),
    "--pc-root",
    str(args.pc_root),
    "--usb-root",
    str(args.usb_root),
    "--db-name",
    args.db_name,
    "--log",
    str(args.log),
]

if args.dry_run:
    cmd.append("--dry-run")

# Añadir cualquier argumento extra que el usuario pase
cmd += extra

# ====================================
# Ejecutar main.py
# ====================================
print(f"Ejecutando comando:\n{' '.join(cmd)}\n")

result = subprocess.run(cmd)

# Retornar mismo código de salida que main.py
sys.exit(result.returncode)
