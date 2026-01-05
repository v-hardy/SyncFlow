import os
import shutil
import psutil  # para detección automática
import time
import hashlib
import sqlite3
from pathlib import Path
from typing import Dict, Tuple, Set, List

class SyncEngine:
# <======================================= INICIAR OBJETO =======================================>    
    def __init__(self, pc_root: Path, usb_root: Path, db_name: str = "metadata.db"):
        self.pc_root = pc_root.resolve()
        self.usb_root = usb_root.resolve()
        self.db_pc_path = self.pc_root / ".sync" / db_name
        self.db_usb_path = self.usb_root / db_name
        self.temp_db_path = self.usb_root / f"{db_name}.tmp"
        
        # Asegurar carpeta oculta en PC
        (self.pc_root / ".sync").mkdir(exist_ok=True)

# <======================================= OBTENER HASH =======================================>
    def calculate_sha256(self, path: Path, chunk_size: int = 8192) -> str:
        sha256 = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

# <======================================= GENERAR DICCIONARIO CON METADATOS =======================================>
    def scan_directory(self, root: Path) -> Dict[str, Tuple[int, float, str]]:
        """
        Devuelve dict: 
        CLAVE: rel_path -> VALOR: (size, mtime, hash_or_none)
        hash_or_none: solo si es necesario más tarde. Por defecto: None
        """
        snapshot = {}
        for file_path in root.rglob("*"):  # .rglob("*") es un método recursivo que busca todos los elementos (archivos y subdirectorios). Es equivalente a glob("**/*", recursive=True), pero usando el estilo de pathlib.
            if file_path.is_file():  # .is_file(): Filtra solo los archivos reales (excluye directorios, enlaces simbólicos que apunten a directorios, etc.). Ignora carpetas vacías o subdirectorios.
                rel_path = str(file_path.relative_to(root).as_posix())  # .relative_to(root) → devuelve un Path relativo (ej. sub/carpeta/archivo.txt). y .as_posix() → convierte a string usando barras / (formato POSIX), incluso en Windows. Esto asegura que el snapshot sea portable entre sistemas operativos.
                stat = file_path.stat()  # .stat obtiene metadatos del archivo
                snapshot[rel_path] = (stat.st_size, stat.st_mtime, None)  # hash calculado bajo demanda
        return snapshot
    
# <======================================= INICIALIZAR DB =======================================>
    def create_schema(self, conn):
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS files (
            rel_path TEXT PRIMARY KEY,
            hash TEXT NOT NULL,
            size INTEGER NOT NULL,
            mtime REAL NOT NULL,
            deleted INTEGER DEFAULT 0,
            deleted_at REAL
        );
        CREATE INDEX IF NOT EXISTS idx_deleted ON files(deleted);
        """)
        conn.commit()

# <======================================= ESTABLECER CONEXION A DB =======================================>
    def get_db_connection(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row  # se devuelven como objetos tipo sqlite3.Row, que funcionan como un diccionario + tupla híbrido. Se accede a las columnas tanto por índice como por nombre, más legible y seguro
        self.create_schema(conn)  # Creo tabla de registros SQL solo si no existe aun
        return conn  # Retorno objeto conexion

# <======================================= CARGAR DB =======================================>
    def load_snapshot_from_db(self, db_path: Path) -> Tuple[Dict, Dict]:
        """
        Devuelve (live_files, tombstones)
        live_files: rel_path -> (size, mtime, hash)
        tombstones: rel_path -> deleted_at
        """
        if not db_path.exists():
            return {}, {}
        
        conn = self.get_db_connection(db_path)
        cursor = conn.cursor()
        
        live = {}
        cursor.execute("SELECT rel_path, size, mtime, hash FROM files WHERE deleted = 0")
        for row in cursor.fetchall():
            live[row['rel_path']] = (row['size'], row['mtime'], row['hash'])
        
        tombstones = {}
        cursor.execute("SELECT rel_path, deleted_at FROM files WHERE deleted = 1")
        for row in cursor.fetchall():
            tombstones[row['rel_path']] = row['deleted_at']
        
        conn.close()
        return live, tombstones

# <======================================= ACTUALIZAR DB =======================================>
    def update_db_after_sync(self, pc_current: Dict, usb_current: Dict):
        # Combinar estado final (deberían ser idénticos)
        final_snapshot = {}
        all_rel_paths = set(pc_current) | set(usb_current)

        for rel_path in all_rel_paths:
            pc_path = self.pc_root / rel_path
            if pc_path.exists():
                hash_val = self.calculate_sha256(pc_path)
                size, mtime, _ = pc_current[rel_path]
            else:
                # Caso raro: solo en USB después de sync (quizá se copio manualmente adrede)
                usb_path = self.usb_root / rel_path
                if usb_path.exists():
                    hash_val = self.calculate_sha256(usb_path)
                    size, mtime, _ = usb_current[rel_path]
                else:
                    continue  # no existe en ninguno, ignorar
            final_snapshot[rel_path] = (size, mtime, hash_val)

        # Escribir DB atómicamente en USB y copia en PC
        temp_conn = self.get_db_connection(self.temp_db_path)
        try:
            temp_conn.execute("DELETE FROM files")

            # Insertar archivos actuales
            for rel_path, (size, mtime, hash_val) in final_snapshot.items():
                temp_conn.execute("""
                    INSERT OR REPLACE INTO files (rel_path, hash, size, mtime, deleted, deleted_at)
                    VALUES (?, ?, ?, ?, 0, NULL)
                """, (rel_path, hash_val, size, mtime))

            # Marcar borrados (solo desde DB anterior en USB)
            all_previous = set()
            if self.db_usb_path.exists():
                prev_conn = sqlite3.connect(f"file:{self.db_usb_path}?mode=ro", uri=True)  # prev_conn = sqlite3.connect(str(self.db_usb_path))
                all_previous = {row[0] for row in prev_conn.execute("SELECT rel_path FROM files WHERE deleted = 0")}
                prev_conn.close()

                for rel_path in all_previous - final_snapshot.keys():  # for rel_path in all_previous - set(final_snapshot):
                    temp_conn.execute("""
                        INSERT OR REPLACE INTO files (rel_path, hash, size, mtime, deleted, deleted_at)
                        VALUES (?, '', 0, 0, 1, ?)
                    """, (rel_path, time.time()))

            temp_conn.commit()
        finally:
            temp_conn.close()
        #Sin manejo de concurrencia

        # Escritura atómica
        try:
            if self.db_usb_path.exists():
                self.db_usb_path.unlink()
            self.temp_db_path.replace(self.db_usb_path)
        except Exception as e:
            # Log error, quizá restaurar temp o algo
            raise RuntimeError(f"Fallo al escribir DB en USB: {e}")

        # Copia de respaldo en PC
        try:
            shutil.copy2(str(self.db_usb_path), str(self.db_pc_path))
        except Exception as e:
            # No crítico, pero loguear
            print(f"Warning: No se pudo copiar backup a PC: {e}")

# <======================================= LISTAR ACCIONES =======================================>
    def decide_actions(self, pc_current: Dict, usb_current: Dict, db_live: Dict, db_tombstones: Dict):
        actions = {
            'copy_pc_to_usb': [],      # (src_path, dst_path, rel_path)
            'copy_usb_to_pc': [],
            'delete_pc': [],           # rel_path
            'delete_usb': [],
            'conflict': []             # (rel_path, pc_path, usb_path)
        }

        all_paths = set(pc_current) | set(usb_current) | set(db_live) | set(db_tombstones)

        for rel_path in all_paths:
            pc_entry = pc_current.get(rel_path)
            usb_entry = usb_current.get(rel_path)
            db_entry = db_live.get(rel_path)
            db_tombstone_entry = db_tombstones.get(rel_path)

            # Caso 1: Eliminado en un lado (tombstone o ausencia física con entrada en DB)
            if db_tombstone_entry: 
                if pc_entry:
                    actions['delete_pc'].append(rel_path)
                if usb_entry:
                    actions['delete_usb'].append(rel_path)
                continue

            # Caso 2: Nuevo en ambos lados con mismo nombre
            if pc_entry and usb_entry and not db_entry:
                pc_size, pc_mtime, _ = pc_entry
                usb_size, usb_mtime, _ = usb_entry
                if pc_size != usb_size or abs(pc_mtime - usb_mtime) > 1:  # tolerancia 1s
                    # Calcular hashes para confirmar
                    pc_hash = self.calculate_sha256(self.pc_root / rel_path)
                    usb_hash = self.calculate_sha256(self.usb_root / rel_path)
                    if pc_hash != usb_hash:
                        actions['conflict'].append((rel_path, self.pc_root / rel_path, self.usb_root / rel_path))
                # Si hashes iguales → no hacer nada (mismo archivo creado independientemente)
                continue

            # Caso 3: Solo en PC
            if pc_entry and not usb_entry and not db_entry:
                actions['copy_pc_to_usb'].append((self.pc_root / rel_path, self.usb_root / rel_path, rel_path))
                continue

            # Caso 4: Solo en USB
            if usb_entry and not pc_entry and not db_entry:
                actions['copy_usb_to_pc'].append((self.usb_root / rel_path, self.pc_root / rel_path, rel_path))
                continue

            # Caso 5: Existe en DB y en ambos lados → posible modificación
            if db_entry and pc_entry and usb_entry:
                db_size, db_mtime, db_hash = db_entry
                pc_size, pc_mtime, _ = pc_entry
                usb_size, usb_mtime, _ = usb_entry

                pc_changed = (pc_size != db_size) or (abs(pc_mtime - db_mtime) > 1)
                usb_changed = (usb_size != db_size) or (abs(usb_mtime - db_mtime) > 1)

                if pc_changed and usb_changed:
                    # Conflicto real
                    actions['conflict'].append((rel_path, self.pc_root / rel_path, self.usb_root / rel_path))
                elif pc_changed:
                    actions['copy_pc_to_usb'].append((self.pc_root / rel_path, self.usb_root / rel_path, rel_path))
                elif usb_changed:
                    actions['copy_usb_to_pc'].append((self.usb_root / rel_path, self.pc_root / rel_path, rel_path))
                # Si ninguno cambió → nada
                continue

            # Caso 6: Eliminado físicamente en un lado pero existe en DB → propagar eliminación
            if db_entry:
                if not pc_entry and usb_entry:
                    actions['delete_pc'].append(rel_path)
                elif not usb_entry and pc_entry:
                    actions['delete_usb'].append(rel_path)

        return actions

# <======================================= EJECUTAR ACCIONES =======================================>
    def execute_actions(self, actions: dict, max_retries: int = 3):
        # 1. Eliminaciones primero
        for rel_path in actions['delete_pc']:
            (self.pc_root / rel_path).unlink(missing_ok=True)
        for rel_path in actions['delete_usb']:
            (self.usb_root / rel_path).unlink(missing_ok=True)

        # 2. Conflictos: renombrar guardando ambas versiones
        for rel_path, pc_path, usb_path in actions['conflict']:
            pc_new = self.pc_root / f"{rel_path}.conflict_PC_{int(time.time())}"
            usb_new = self.usb_root / f"{rel_path}.conflict_USB_{int(time.time())}"
            if pc_path.exists():
                shutil.move(str(pc_path), str(pc_new))
            if usb_path.exists():
                shutil.move(str(usb_path), str(usb_new))
            print(f"CONFLICTO resuelto: {rel_path} → versiones guardadas")

        # 3. Copias con verificación y reintentos
        for src, dst, rel_path in actions['copy_pc_to_usb'] + actions['copy_usb_to_pc']:
            dst.parent.mkdir(parents=True, exist_ok=True)
            success = False
            for attempt in range(max_retries):
                try:
                    shutil.copy2(src, dst)
                    # Verificar hash inmediatamente
                    if self.calculate_sha256(src) == self.calculate_sha256(dst):
                        success = True
                        break
                    else:
                        dst.unlink(missing_ok=True)  # Corrupto → reintentar
                except Exception as e:
                    print(f"Error copia {rel_path}: {e}")
                    # REGISTRAR EN LOG
                    time.sleep(1)
            if not success:
                raise RuntimeError(f"Falló copia definitiva de {rel_path} tras {max_retries} intentos")

# <======================================= Sincronizacion bidireccional =======================================>
    def sync(self):
        print("Iniciando sincronización bidireccional...")

        # Cargar DB (prioridad: USB, fallback PC)
        db_path = self.db_usb_path if self.db_usb_path.exists() else self.db_pc_path
        db_live, db_tombstones = self.load_snapshot_from_db(db_path)

        # Escanear estados actuales
        pc_current = self.scan_directory(self.pc_root)
        usb_current = self.scan_directory(self.usb_root)

        # Decidir acciones
        actions = self.decide_actions(pc_current, usb_current, db_live, db_tombstones)

        # Ejecutar
        self.execute_actions(actions)

        # Actualizar DB solo si todo salió bien
        self.update_db_after_sync(pc_current, usb_current)

        print("Sincronización completada exitosamente.")

# <======================================= Detección automática con fallback =======================================>
def main():
    pc_root = Path.home() / "Datos"

    # Intentar detección automática
    try:
        drives = [Path(p.mountpoint) for p in psutil.disk_partitions()
                if 'removable' in p.opts.lower()]
        if len(drives) == 1:
            usb_root = drives[0]
        elif len(drives) > 1:
            raise ValueError("Múltiples pendrives detectados")
        else:
            raise ValueError("Ningún pendrive encontrado")
    except Exception as e:
        print(e)
        usb_root = Path(input("Introduce la ruta raíz del pendrive (ej. /media/usuario/PENDRIVE o E:\\): ")).resolve()

    engine = SyncEngine(pc_root=pc_root, usb_root=usb_root)
    engine.sync()

# ====================
# PROCESO
# ====================
if __name__ == "__main__":
    main()
