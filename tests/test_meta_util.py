import hashlib
import pytest
from pathlib import Path
from sync.meta_util import sha256_file, walk_directory_metadata

# ------------------ TEST PARA sha256_file ------------------


def test_sha256_file(tmp_path: Path):
    # Crear un archivo de prueba
    file_path = tmp_path / "test.txt"
    content = b"Hola mundo"
    file_path.write_bytes(content)

    # Calcular hash esperado
    expected_hash = hashlib.sha256(content).hexdigest()

    # Test de la función
    assert sha256_file(file_path) == expected_hash


def test_sha256_file_nonexistent(tmp_path: Path):
    # Archivo que no existe
    file_path = tmp_path / "no_existe.txt"

    # Debe lanzar FileNotFoundError
    with pytest.raises(FileNotFoundError):
        sha256_file(file_path)


# ------------------ TEST PARA walk_directory_metadata ------------------


def test_walk_directory_metadata_single_file(tmp_path: Path):
    file_path = tmp_path / "file1.txt"
    content = b"abc"
    file_path.write_bytes(content)

    snapshot = walk_directory_metadata(tmp_path)
    rel_path = "file1.txt"
    assert rel_path in snapshot
