from unittest.mock import MagicMock, patch
from sync.engine import EngineSync


def test_phase2_detect_create(tmp_path):
    engine = EngineSync(tmp_path, tmp_path, "test.db")

    engine.db.read_states = MagicMock(return_value=[])
    engine.db.table_is_empty = MagicMock(return_value=False)

    with (
        patch("sync.engine.walk_directory_metadata") as walk_mock,
        patch("sync.engine.sha256_file") as hash_mock,
        patch("sync.database.DB.upsert_movement") as upsert_mock,
    ):
        walk_mock.return_value = {"new.txt": (100, 1234, None)}
        hash_mock.return_value = "hash123"

        engine.get_movements()

    upsert_mock.assert_called_once()
    assert upsert_mock.call_args[0][1]["op_type"] == "CREATE"
