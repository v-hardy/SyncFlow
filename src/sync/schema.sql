PRAGMA foreign_keys = ON;

-- ===============================
-- Maestro de estado de archivos
-- ===============================
CREATE TABLE IF NOT EXISTS master_states (
    init_hash      TEXT PRIMARY KEY,
    rel_path       TEXT NOT NULL,
    content_hash   TEXT NOT NULL,
    size_bytes     INTEGER NOT NULL,
    last_op_time   INTEGER NOT NULL,
    machine_name   TEXT NOT NULL
);

-- ===============================
-- Tombstones (borrados)
-- ===============================
CREATE TABLE IF NOT EXISTS tombstones (
    init_hash       TEXT PRIMARY KEY,
    content_hash    TEXT NOT NULL,
    deleted_at      INTEGER NOT NULL,
    machine_name    TEXT NOT NULL
);

-- ===============================
-- Movimientos (delta)
-- ===============================
CREATE TABLE IF NOT EXISTS movements (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    init_hash       TEXT,
    op_type         TEXT NOT NULL CHECK(op_type IN ('CREATE','MODIFY','MOVE','DELETE')),
    rel_path        TEXT,
    new_rel_path    TEXT,
    content_hash    TEXT,
    size_bytes      INTEGER NOT NULL,
    last_op_time    INTEGER NOT NULL,
    machine_name    TEXT NOT NULL,
    --
    CHECK (
        (op_type = 'CREATE' AND rel_path IS NOT NULL AND new_rel_path IS NULL) OR
        (op_type = 'MODIFY' AND rel_path IS NOT NULL AND new_rel_path IS NULL) OR
        (op_type = 'MOVE'   AND rel_path IS NOT NULL AND new_rel_path IS NOT NULL) OR
        (op_type = 'DELETE' AND rel_path IS NOT NULL AND new_rel_path IS NULL)
    )
);  

-- ===============================
-- Archivo histórico
-- ===============================
CREATE TABLE IF NOT EXISTS movements_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    op_type         TEXT CHECK(op_type IN ('CREATE','MODIFY','MOVE','DELETE')) NOT NULL,
    init_hash       TEXT NOT NULL,
    rel_path        TEXT,
    new_rel_path    TEXT,
    content_hash    TEXT,
    size_bytes      INTEGER NOT NULL,
    last_op_time    INTEGER NOT NULL,
    machine_name    TEXT NOT NULL,
    applied_time    INTEGER
);
