import os
import sys
import tempfile
import shutil
from pathlib import Path

# Agregar el directorio padre al path para poder importar sync
sys.path.insert(0, str(Path(__file__).parent.parent))

from sync.engine import EngineSync
from sync.database import DB
from sync.fs_util import FSOps
from sync.meta_util import sha256_file, walk_directory_metadata


def test_integration_full_sync():
    """Test de integración completo con directorios reales"""
    
    # Crear directorios temporales
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        pc_root = tmp_path / "pc"
        usb_root = tmp_path / "usb"
        
        pc_root.mkdir()
        usb_root.mkdir()
        
        # Crear engine
        engine = EngineSync(pc_root, usb_root, "test.db")
        
        # =========================
        # FASE 1: Simular USB con archivos
        # =========================
        print("=== FASE 1: Creando archivos en USB ===")
        
        # Crear archivos en USB
        usb_file1 = usb_root / "file1.txt"
        usb_file2 = usb_root / "file2.txt"
        
        FSOps.create_file(usb_file1, b"contenido original 1 con mas texto")
        FSOps.create_file(usb_file2, b"contenido original 2 con mas texto")
        
        # Inicializar DB de USB con los archivos
        with engine.db.get_db_connection(engine.db.usb_path) as conn:
            hash1 = sha256_file(usb_file1)
            hash2 = sha256_file(usb_file2)
            
            import time
            now = int(time.time())
            
            conn.execute(
                """
                INSERT INTO master_states 
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (hash1, "file1.txt", hash1, len(b"contenido original 1 con mas texto"), now, "usb_machine")
            )
            
            conn.execute(
                """
                INSERT INTO master_states 
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (hash2, "file2.txt", hash2, len(b"contenido original 2 con mas texto"), now, "usb_machine")
            )
            
            conn.commit()
        
        # =========================
        # FASE 2: Replicar desde USB a PC
        # =========================
        print("=== FASE 2: Replicando desde USB a PC ===")
        engine.replicate_master()
        
        # Verificar que los archivos se copiaron
        assert (pc_root / "file1.txt").exists()
        assert (pc_root / "file2.txt").exists()
        
        # Verificar contenido
        assert (pc_root / "file1.txt").read_bytes() == b"contenido original 1 con mas texto"
        assert (pc_root / "file2.txt").read_bytes() == b"contenido original 2 con mas texto"
        
        # =========================
        # FASE 3: Modificar archivo en PC
        # =========================
        print("=== FASE 3: Modificando archivo en PC ===")
        pc_file1 = pc_root / "file1.txt"
        FSOps.modify_file(pc_file1, b"contenido modificado con mas texto para cambiar tamano")
        
        # =========================
        # FASE 4: Detectar cambios
        # =========================
        print("=== FASE 4: Detectando cambios locales ===")
        
        # Debug: ver qué hay en master_states antes de detectar cambios
        with engine.db.get_db_connection(engine.db.pc_path) as conn:
            master_before = engine.db.read_states(conn)
            print(f"Master states antes de detectar cambios: {len(master_before)}")
            for m in master_before:
                print(f"  - {m['rel_path']}: {m['content_hash']}")
        
        # Debug: ver qué hay en el filesystem
        tree = walk_directory_metadata(pc_root)
        print(f"Archivos en filesystem: {len(tree)}")
        for path, (size, mtime, _) in tree.items():
            print(f"  - {path}: {size} bytes, mtime={mtime}")
        
        engine.get_movements()
        
        # Verificar que se detectó el movimiento
        with engine.db.get_db_connection(engine.db.temp_path) as conn:
            movements = engine.db.read_movements(conn)
            print(f"Movimientos detectados: {len(movements)}")
            for mov in movements:
                print(f"  - {mov['op_type']}: {mov['rel_path']}")
            
            assert len(movements) > 0, "No se detectaron movimientos"
            
            modify_movements = [m for m in movements if m["op_type"] == "MODIFY"]
            assert len(modify_movements) > 0, "No se detectaron modificaciones"
        
        # =========================
        # FASE 5: Aplicar movimientos a USB
        # =========================
        print("=== FASE 5: Aplicando movimientos a USB ===")
        engine.apply_movements()
        
        # Verificar que el archivo se modificó en USB
        usb_content = (usb_root / "file1.txt").read_bytes()
        expected = b"contenido modificado con mas texto para cambiar tamano"
        print(f"Contenido USB: {usb_content}")
        print(f"Esperado: {expected}")
        assert usb_content == expected
        
        # =========================
        # FASE 6: Crear nuevo archivo en PC
        # =========================
        print("=== FASE 6: Creando nuevo archivo en PC ===")
        pc_file3 = pc_root / "file3.txt"
        FSOps.create_file(pc_file3, b"nuevo archivo")
        
        # Detectar cambios
        engine.get_movements()
        
        # Aplicar movimientos
        engine.apply_movements()
        
        # Verificar que el nuevo archivo se copió a USB
        assert (usb_root / "file3.txt").exists()
        assert (usb_root / "file3.txt").read_bytes() == b"nuevo archivo"
        
        # =========================
        # FASE 7: Borrar archivo en PC
        # =========================
        print("=== FASE 7: Borrando archivo en PC ===")
        FSOps.delete_file(pc_root / "file2.txt")
        
        # Detectar cambios
        engine.get_movements()
        
        # Aplicar movimientos
        engine.apply_movements()
        
        # Verificar que el archivo se borró de USB
        assert not (usb_root / "file2.txt").exists()
        
        print("=== TEST DE INTEGRACIÓN COMPLETADO EXITOSAMENTE ===")


def test_integration_dry_run():
    """Test de integración para dry-run"""
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        pc_root = tmp_path / "pc"
        usb_root = tmp_path / "usb"
        
        pc_root.mkdir()
        usb_root.mkdir()
        
        # Crear archivos
        usb_file = usb_root / "file.txt"
        FSOps.create_file(usb_file, b"contenido")
        
        # Inicializar DB
        engine = EngineSync(pc_root, usb_root, "test.db")
        
        with engine.db.get_db_connection(engine.db.usb_path) as conn:
            import time
            hash_val = sha256_file(usb_file)
            now = int(time.time())
            
            conn.execute(
                """
                INSERT INTO master_states 
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (hash_val, "file.txt", hash_val, len(b"contenido"), now, "usb_machine")
            )
            conn.commit()
        
        # Ejecutar dry-run
        from sync.dry_run import dry_run
        stats = dry_run(engine, print)
        
        assert stats is not None
        print(f"Estadísticas del dry-run: {stats}")
        
        # Verificar que no se aplicaron cambios reales
        assert not (pc_root / "file.txt").exists()
        
        print("=== TEST DRY-RUN COMPLETADO ===")


if __name__ == "__main__":
    print("Ejecutando tests de integración...")
    test_integration_full_sync()
    test_integration_dry_run()
    print("Todos los tests de integración pasaron correctamente.")