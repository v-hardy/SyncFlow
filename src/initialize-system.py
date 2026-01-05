import sqlite3
import os
import hashlib
import time
import shutil
from pathlib import Path

def get_db_connection(db_path: str):
    """Conecta y asegura que el esquema exista"""
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    create_schema(conn)
    return conn

def create_schema(conn):
    """Crea las tablas e índices si no existen"""
    cursor = conn.cursor()
    with open('schema.sql', 'r', encoding='utf-8') as f:  # Guarda el SQL anterior en un archivo schema.sql
        cursor.executescript(f.read())
    conn.commit()

# Ejemplo: Insertar o actualizar un archivo (vivo)
def upsert_file(conn, rel_path: str, file_path: Path):
    hash_hex = calculate_sha256(file_path)
    stat = file_path.stat()
    with conn:
        conn.execute("""
            INSERT INTO files (rel_path, hash, size, mtime, deleted, deleted_at, synced_at)
            VALUES (?, ?, ?, ?, 0, NULL, ?)
            ON CONFLICT(rel_path) DO UPDATE SET
                hash=excluded.hash,
                size=excluded.size,
                mtime=excluded.mtime,
                deleted=0,
                deleted_at=NULL,
                synced_at=excluded.synced_at
        """, (rel_path, hash_hex, stat.st_size, stat.st_mtime, time.time()))

# Ejemplo: Marcar como borrado (tombstone)
def mark_deleted(conn, rel_path: str):
    with conn:
        conn.execute("""
            UPDATE files SET deleted=1, deleted_at=? WHERE rel_path=?
        """, (time.time(), rel_path))

# Ejemplo: Obtener snapshot actual (solo archivos vivos)
def get_live_snapshot(conn):
    cursor = conn.execute("SELECT rel_path, hash, size, mtime FROM files WHERE deleted=0")
    return {row['rel_path']: dict(row) for row in cursor.fetchall()}

# Ejemplo: Obtener tombstones pendientes (borrados después de cierta fecha)
def get_tombstones_since(conn, since_timestamp: float):
    cursor = conn.execute("""
        SELECT rel_path, deleted_at FROM files
        WHERE deleted=1 AND deleted_at > ?
    """, (since_timestamp,))
    return {row['rel_path']: row['deleted_at'] for row in cursor.fetchall()}

def calculate_sha256(file_path: Path, chunk_size=8192):
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            sha256.update(chunk)
    return sha256.hexdigest()
