from enum import Enum, unique
from typing import Any, Callable, Union


class TawaziBaseException(BaseException):
    pass


class TawaziArgumentException(TawaziBaseException):
    def __init__(self, func_name: str, arg_name: str) -> None:
        msg = f"Argument {arg_name} wasn't passed for the DAG" f" created from function {func_name}"
        super().__init__(msg)


class TawaziTypeError(TawaziBaseException):
    pass


class TawaziUsageError(TawaziBaseException):
    pass


def raise_arg_exc(func_or_func_name: Union[str, Callable[[Any], Any]], arg_name: str) -> None:
    if isinstance(func_or_func_name, str):
        raise TawaziArgumentException(func_or_func_name, arg_name)

    raise TawaziArgumentException(func_or_func_name.__qualname__, arg_name)


class InvalidExecNodeCall(TawaziBaseException):
    pass


@unique
class ErrorStrategy(str, Enum):
    # supported behavior following a raised error
    strict: str = "strict"
    all_children: str = "all-children"
    permissive: str = "permissive"
