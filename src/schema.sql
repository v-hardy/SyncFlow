-- Tabla principal de archivos de metadata.db
CREATE TABLE IF NOT EXISTS files (
    rel_path TEXT PRIMARY KEY,          -- Ruta relativa, ej: "documentos/tesis.pdf"
    hash TEXT NOT NULL,                 -- SHA-256 en hex
    size INTEGER NOT NULL,              -- Tamaño en bytes
    mtime REAL NOT NULL,                -- Timestamp de última modificación (float UNIX)
    deleted INTEGER DEFAULT 0,          -- 0 = vivo, 1 = borrado (tombstone)
    deleted_at REAL,                    -- Timestamp del borrado (para limpieza futura)
    synced_at REAL                      -- Timestamp de última sincronización exitosa (opcional, útil para logs)
);

-- Índices para acelerar las consultas más comunes
CREATE INDEX IF NOT EXISTS idx_mtime ON files(mtime);
CREATE INDEX IF NOT EXISTS idx_size ON files(size);
CREATE INDEX IF NOT EXISTS idx_hash ON files(hash);
CREATE INDEX IF NOT EXISTS idx_deleted ON files(deleted);
CREATE INDEX IF NOT EXISTS idx_synced_at ON files(synced_at);

-- Tabla opcional para metadata global (versión del esquema, última sync completa, etc.)
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- Insertar versión inicial del esquema
INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', '1');
INSERT OR IGNORE INTO meta (key, value) VALUES ('last_full_sync', '0');
