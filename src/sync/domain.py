class MovementRules:
    @staticmethod
    def can_apply(mov: dict, current_state_in_master: set):
        op = mov["op_type"]

        if op == "CREATE":
            return not current_state_in_master.exists(mov["new_rel_path"])

        elif op == "MODIFY":
            return current_state_in_master.exists(mov["rel_path"])

        elif op == "MOVE":
            return current_state_in_master.exists(
                mov["rel_path"]
            ) and not current_state_in_master.exists(mov["new_rel_path"])

        elif op == "DELETE":
            return current_state_in_master.exists(mov["rel_path"])

        return False


class CurrentState:
    def __init__(self, paths):
        self._paths = set(paths)

    def exists(self, rel_path):
        return rel_path in self._paths


""" 
    # Transformar en una estructura auxiliar para validar el cambio FS, para confirmar modif de DB
    def apply(self, mov: dict):
        op = mov["op_type"]

        if op == "CREATE":
            # se asume que can_apply ya validó
            self._paths.add(mov["new_rel_path"])

        elif op == "MODIFY":
            # no cambia la estructura del FS
            pass

        elif op == "MOVE":
            self._paths.remove(mov["rel_path"])
            self._paths.add(mov["new_rel_path"])

        elif op == "DELETE":
            self._paths.remove(mov["rel_path"])
 """
