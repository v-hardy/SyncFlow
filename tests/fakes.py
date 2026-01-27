class FakeFS:
    def __init__(self):
        self.ops = []

    def copy_file(self, src, dst):
        self.ops.append(("copy", src, dst))

    def move_file(self, src, dst):
        self.ops.append(("move", src, dst))

    def delete_file(self, path):
        self.ops.append(("delete", path))


class FakeDB:
    def __init__(self):
        self.pc_states = []
        self.usb_states = []
        self.temp_states = []

        self.movements = []
        self.tombstones = []

        self.pc_path = "pc"
        self.usb_path = "usb"
        self.temp_path = "temp"

        self._current_path = None

    def get_db_connection(self, path):
        self._current_path = path
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._current_path = None

    # ---------- helpers ----------

    def _states_for_current_path(self):
        if self._current_path == self.pc_path:
            return self.pc_states
        if self._current_path == self.usb_path:
            return self.usb_states
        return self.temp_states

    # ---------- DB API ----------

    def table_is_empty(self, _, table):
        if table == "master_states":
            return len(self._states_for_current_path()) == 0
        if table == "movements":
            return len(self.movements) == 0
        return True

    def read_states(self, *_):
        return self._states_for_current_path()

    def read_tombstones(self, *_):
        return self.tombstones

    def read_movements(self, *_):
        return self.movements

    def upsert_movement(self, _, mov):
        self.movements.append(mov)

    def update_state(self, _, mov):
        pass

    def archive_and_delete_movement(self, _, mov):
        self.movements.remove(mov)
