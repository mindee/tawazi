from enum import Enum, unique
from typing import Any, Callable


class TawaziBaseException(BaseException):
    pass


class TawaziArgumentException(TawaziBaseException):
    def __init__(self, func: Callable[[Any], Any], arg_name: str) -> None:
        msg = (
            f"Argument {arg_name} wasn't passed for the DAG"
            f" created from function {func.__qualname__}"
        )
        super().__init__(msg)


def raise_arg_exc(func: Callable[[Any], Any], arg_name: str) -> None:
    raise TawaziArgumentException(func, arg_name)


class UnvalidExecNodeCall(TawaziBaseException):
    pass


@unique
class ErrorStrategy(str, Enum):
    # supported behavior following a raised error
    strict: str = "strict"
    all_children: str = "all-children"
    permissive: str = "permissive"
