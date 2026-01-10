PRAGMA foreign_keys = ON;

-- =========================
-- Tabla principal de archivos
-- =========================
CREATE TABLE IF NOT EXISTS files (
    init_hash TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    rel_path TEXT NOT NULL,
    timestamp INTEGER NOT NULL
);

-- =========================
-- Tombstones (borrados)
-- =========================
CREATE TABLE IF NOT EXISTS tombstones (
    init_hash TEXT PRIMARY KEY,
    deleted_at INTEGER NOT NULL,
    machine_name TEXT NOT NULL
);

-- =========================
-- Movimientos (delta)
-- =========================
CREATE TABLE IF NOT EXISTS movements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    op_type TEXT CHECK(op_type IN ('CREATE','MODIFY','MOVE','DELETE')) NOT NULL,
    init_hash TEXT NOT NULL,
    old_rel_path TEXT,
    new_rel_path TEXT,
    content_hash TEXT,
    op_time INTEGER NOT NULL,
    machine_name TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mov_init_hash ON movements(init_hash);

-- =========================
-- Archivo histórico
-- =========================
CREATE TABLE IF NOT EXISTS movements_archive AS
SELECT *, NULL AS applied_at FROM movements WHERE 0;

