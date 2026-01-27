import pytest
from sync.domain import MovementRules, CurrentState

# ------------------ FIXTURE DE ESTADO INICIAL ------------------


@pytest.fixture
def initial_state():
    return CurrentState(paths={"file1.txt", "file2.txt"})


# ------------------ TEST PARA CREATE ------------------


def test_can_apply_create(initial_state):
    mov = {"op_type": "CREATE", "new_rel_path": "file3.txt"}
    # file3.txt no existe → debe poder aplicarse
    assert MovementRules.can_apply(mov, initial_state) is True

    mov_existing = {"op_type": "CREATE", "new_rel_path": "file1.txt"}
    # file1.txt ya existe → no se puede crear
    assert MovementRules.can_apply(mov_existing, initial_state) is False


# ------------------ TEST PARA MODIFY ------------------


def test_can_apply_modify(initial_state):
    mov = {"op_type": "MODIFY", "rel_path": "file1.txt"}
    # file1.txt existe → se puede modificar
    assert MovementRules.can_apply(mov, initial_state) is True

    mov_nonexistent = {"op_type": "MODIFY", "rel_path": "file3.txt"}
    # file3.txt no existe → no se puede modificar
    assert MovementRules.can_apply(mov_nonexistent, initial_state) is False


# ------------------ TEST PARA MOVE ------------------


def test_can_apply_move(initial_state):
    mov = {"op_type": "MOVE", "rel_path": "file1.txt", "new_rel_path": "file3.txt"}
    # file1.txt existe y file3.txt no → se puede mover
    assert MovementRules.can_apply(mov, initial_state) is True

    mov_target_exists = {
        "op_type": "MOVE",
        "rel_path": "file1.txt",
        "new_rel_path": "file2.txt",
    }
    # file2.txt ya existe → no se puede mover
    assert MovementRules.can_apply(mov_target_exists, initial_state) is False

    mov_source_missing = {
        "op_type": "MOVE",
        "rel_path": "fileX.txt",
        "new_rel_path": "file3.txt",
    }
    # fileX.txt no existe → no se puede mover
    assert MovementRules.can_apply(mov_source_missing, initial_state) is False


# ------------------ TEST PARA DELETE ------------------


def test_can_apply_delete(initial_state):
    mov = {"op_type": "DELETE", "rel_path": "file1.txt"}
    # file1.txt existe → se puede borrar
    assert MovementRules.can_apply(mov, initial_state) is True

    mov_nonexistent = {"op_type": "DELETE", "rel_path": "fileX.txt"}
    # fileX.txt no existe → no se puede borrar
    assert MovementRules.can_apply(mov_nonexistent, initial_state) is False


# ------------------ TEST PARA OPERACIÓN DESCONOCIDA ------------------


def test_can_apply_unknown(initial_state):
    mov = {"op_type": "UNKNOWN", "rel_path": "file1.txt"}
    # operación desconocida → False
    assert MovementRules.can_apply(mov, initial_state) is False
