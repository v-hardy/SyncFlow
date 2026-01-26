import os
import stat
import time

import pytest

from sync.fs_util import FSOps


def test_ensure_parent_creates_parent_directory(tmp_path):
    file_path = tmp_path / "a" / "b" / "file.txt"

    FSOps.ensure_parent(file_path)

    assert (tmp_path / "a" / "b").is_dir()


def test_create_file_creates_file_with_content(tmp_path):
    file_path = tmp_path / "dir" / "file.bin"
    content = b"hola mundo"

    FSOps.create_file(file_path, content)

    assert file_path.exists()
    assert file_path.read_bytes() == content


def test_modify_file_overwrites_existing_content(tmp_path):
    file_path = tmp_path / "file.txt"
    file_path.write_bytes(b"viejo")

    FSOps.modify_file(file_path, b"nuevo")

    assert file_path.read_bytes() == b"nuevo"


def test_copy_file_copies_content(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "nested" / "dst.txt"
    src.write_bytes(b"contenido")

    FSOps.copy_file(src, dst)

    assert dst.exists()
    assert dst.read_bytes() == b"contenido"


def test_move_file_moves_file(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "nested" / "dst.txt"
    src.write_bytes(b"contenido")

    FSOps.move_file(src, dst)

    assert not src.exists()
    assert dst.exists()
    assert dst.read_bytes() == b"contenido"


def test_delete_file_removes_file(tmp_path):
    file_path = tmp_path / "file.txt"
    file_path.write_bytes(b"data")

    FSOps.delete_file(file_path)

    assert not file_path.exists()


def test_delete_file_does_nothing_if_file_does_not_exist(tmp_path):
    file_path = tmp_path / "nope.txt"

    FSOps.delete_file(file_path)  # no debería explotar

    assert not file_path.exists()


# --- advanced behavior / regression tests ---


def test_copy_file_preserves_mtime(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"

    src.write_bytes(b"contenido")

    # forzamos un mtime conocido
    past_time = time.time() - 3600
    os.utime(src, (past_time, past_time))

    FSOps.copy_file(src, dst)

    src_stat = src.stat()
    dst_stat = dst.stat()

    assert int(src_stat.st_mtime) == int(dst_stat.st_mtime)


@pytest.mark.skipif(os.name == "nt", reason="Permisos POSIX no aplican en Windows")
def test_move_file_preserves_permissions(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"

    src.write_bytes(b"contenido")
    src.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 600

    FSOps.move_file(src, dst)

    mode = dst.stat().st_mode
    assert stat.S_IMODE(mode) == 0o600


def test_ensure_parent_is_idempotent(tmp_path):
    path = tmp_path / "a" / "b" / "file.txt"
    (tmp_path / "a" / "b").mkdir(parents=True)

    FSOps.ensure_parent(path)
    FSOps.ensure_parent(path)  # segunda vez

    assert (tmp_path / "a" / "b").is_dir()


def test_modify_file_fails_if_file_does_not_exist(tmp_path):
    file_path = tmp_path / "missing.txt"

    with pytest.raises(FileNotFoundError):
        FSOps.modify_file(file_path, b"data")


def test_delete_file_does_not_delete_directory(tmp_path):
    dir_path = tmp_path / "dir"
    dir_path.mkdir()

    with pytest.raises(Exception):
        FSOps.delete_file(dir_path)

    assert dir_path.exists()
