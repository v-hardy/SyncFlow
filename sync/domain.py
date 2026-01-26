import logging

logger = logging.getLogger(__name__)


class MovementRules:
    @staticmethod
    def can_apply(mov: dict, current_state_in_master: set):
        op = mov["op_type"]

        logger.debug(
            "Evaluando movimiento: op=%s rel=%s new_rel=%s",
            op,
            mov.get("rel_path"),
            mov.get("new_rel_path"),
        )

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

        else:
            logger.debug("Operación desconocida: %s", op)
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

        logger.debug("Aplicando movimiento a estado actual: %s", mov)

        if op == "CREATE":
            self._paths.add(mov["new_rel_path"])

        elif op == "MODIFY":
            pass

        elif op == "MOVE":
            self._paths.remove(mov["rel_path"])
            self._paths.add(mov["new_rel_path"])

        elif op == "DELETE":
            self._paths.remove(mov["rel_path"])

        else:
            logger.warning("Movimiento con op desconocida: %s", op)

 """
