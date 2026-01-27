from unittest.mock import MagicMock, patch
from sync.engine import EngineSync


def test_phase1_copy_from_usb(tmp_path):
    engine = EngineSync(tmp_path, tmp_path, "test.db")

    engine._read_usb_master = MagicMock(
        return_value=(
            [
                {
                    "init_hash": "abc",
                    "rel_path": "file.txt",
                    "mtime": 10,
                    "content_hash": "hash1",
                }
            ],
            [],
        )
    )

    engine._read_pc_master = MagicMock(return_value=[])

    with patch("sync.engine.FSOps.copy_file") as copy_mock:
        engine.replicate_master()

    copy_mock.assert_called_once()
