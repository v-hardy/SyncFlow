import sqlite3
import pytest
from sync.database import DB


@pytest.fixture
def temp_roots(tmp_path):
    pc = tmp_path / "pc"
    usb = tmp_path / "usb"
    pc.mkdir()
    usb.mkdir()
    return pc, usb


@pytest.fixture
def db(temp_roots):
    pc, usb = temp_roots
    return DB(pc_root=pc, usb_root=usb, db_name="test.db")


@pytest.fixture
def conn(db):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    db.create_schema(conn)
    yield conn
    conn.close()


def test_create_schema_creates_tables(db, conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cur.fetchall()}

    # ajusta según schema.sql real
    assert "master_states" in tables
    assert "movements" in tables
    assert "tombstones" in tables


def test_update_state_create(db, conn):
    mov = {
        "op_type": "CREATE",
        "init_hash": "hash1",
        "rel_path": "a.txt",
        "content_hash": "c1",
        "size_bytes": 10,
        "last_op_time": 123,
        "machine_name": "pc1",
    }

    db.update_state(conn, mov)

    rows = conn.execute("SELECT * FROM master_states").fetchall()
    assert len(rows) == 1
    assert rows[0]["rel_path"] == "a.txt"


def test_update_state_modify(db, conn):
    conn.execute(
        """
        INSERT INTO master_states
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("h1", "a.txt", "old", 1, 100, "pc1"),
    )

    mov = {
        "op_type": "MODIFY",
        "rel_path": "a.txt",
        "content_hash": "new",
        "size_bytes": 20,
        "last_op_time": 200,
        "machine_name": "pc2",
    }

    db.update_state(conn, mov)

    row = conn.execute("SELECT * FROM master_states WHERE rel_path='a.txt'").fetchone()

    assert row["content_hash"] == "new"
    assert row["size_bytes"] == 20


def test_update_state_move(db, conn):
    conn.execute(
        "INSERT INTO master_states VALUES (?, ?, ?, ?, ?, ?)",
        ("h1", "a.txt", "c1", 10, 100, "pc1"),
    )

    mov = {
        "op_type": "MOVE",
        "rel_path": "a.txt",
        "new_rel_path": "b.txt",
    }

    db.update_state(conn, mov)

    row = conn.execute("SELECT * FROM master_states").fetchone()

    assert row["rel_path"] == "b.txt"


def test_update_state_delete(db, conn):
    conn.execute(
        "INSERT INTO master_states VALUES (?, ?, ?, ?, ?, ?)",
        ("h1", "a.txt", "c1", 10, 100, "pc1"),
    )

    mov = {
        "op_type": "DELETE",
        "init_hash": "h1",
        "rel_path": "a.txt",
        "content_hash": "c1",
        "last_op_time": 300,
        "machine_name": "pc1",
    }

    db.update_state(conn, mov)

    assert conn.execute("SELECT * FROM master_states").fetchone() is None

    tomb = conn.execute("SELECT * FROM tombstones").fetchone()

    assert tomb["init_hash"] == "h1"


def test_upsert_movement(db, conn):
    mov = {
        "id": "1",
        "op_type": "CREATE",
        "init_hash": "h1",
        "rel_path": "a.txt",
        "new_rel_path": None,
        "content_hash": "c1",
        "size_bytes": 10,
        "last_op_time": 100,
        "machine_name": "pc1",
    }

    db.upsert_movement(conn, mov)

    row = conn.execute("SELECT * FROM movements WHERE id='1'").fetchone()

    assert row["op_type"] == "CREATE"


def test_archive_and_delete_movement(db, conn):
    conn.execute(
        """
        INSERT INTO movements (
                id, 
                op_type, 
                init_hash, 
                rel_path, 
                new_rel_path,
                content_hash, 
                size_bytes,
                last_op_time, 
                machine_name
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("1", "CREATE", "h1", "a.txt", None, "c1", 10, 100, "pc1"),
    )

    mov = {
        "id": "1",
        "op_type": "CREATE",
        "init_hash": "h1",
        "rel_path": "a.txt",
        "new_rel_path": None,
        "content_hash": "c1",
        "size_bytes": 10,
        "last_op_time": 100,
        "machine_name": "pc1",
    }

    db.archive_and_delete_movement(conn, mov)

    assert conn.execute("SELECT * FROM movements WHERE id='1'").fetchone() is None

    hist = conn.execute("SELECT * FROM movements_history WHERE id='1'").fetchone()

    assert hist is not None
