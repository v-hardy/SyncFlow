from unittest.mock import MagicMock, patch
from sync.engine import EngineSync


def test_phase3_apply_modify(tmp_path):
    engine = EngineSync(tmp_path, tmp_path, "test.db")

    mov = {
        "op_type": "MODIFY",
        "rel_path": "file.txt",
        "init_hash": "abc",
    }

    engine.db.read_movements = MagicMock(return_value=[mov])
    engine.db.read_states = MagicMock(return_value=[{"rel_path": "file.txt"}])
    engine.db.table_is_empty = MagicMock(return_value=False)

    # Mock DB writes
    engine.db.update_state = MagicMock()
    engine.db.archive_and_delete_movement = MagicMock()

    with (
        patch("sync.engine.FSOps.copy_file") as copy_mock,
        patch("sync.engine.MovementRules.can_apply", return_value=True),
    ):
        engine.apply_movements()

    copy_mock.assert_called_once()
    engine.db.update_state.assert_called_once()
    engine.db.archive_and_delete_movement.assert_called_once()
