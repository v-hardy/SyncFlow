from unittest.mock import MagicMock, patch
from sync.engine import EngineSync
from sync.dry_run import dry_run


def test_dry_run_basic(tmp_path):
    engine = EngineSync(tmp_path, tmp_path, "test.db")

    # Mock de los métodos del engine
    engine._read_usb_master = MagicMock(return_value=([], []))
    engine._read_pc_master = MagicMock(return_value=[])

    with patch("sync.dry_run.walk_directory_metadata", return_value={}):
        stats = dry_run(engine, MagicMock())

    assert stats is not None


def test_dry_run_phase1_with_files(tmp_path):
    engine = EngineSync(tmp_path, tmp_path, "test.db")

    usb_master = [
        {
            "init_hash": "abc",
            "rel_path": "file.txt",
            "content_hash": "hash1",
        }
    ]

    engine._read_usb_master = MagicMock(return_value=(usb_master, []))
    engine._read_pc_master = MagicMock(return_value=[])

    with patch("sync.dry_run.walk_directory_metadata", return_value={}):
        stats = dry_run(engine, MagicMock())

    assert stats["initial_copy"] == 1