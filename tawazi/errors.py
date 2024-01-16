"""Module for custom errors raised by Tawazi."""
from typing import Any, Callable, NoReturn, Union


class TawaziBaseException(BaseException):
    """BaseException of Tawazi from which all other exceptions inherit."""

    pass


class TawaziArgumentException(TawaziBaseException):
    """Raised when using Tawazi (Passing the wrong number/type of arguments etc.)."""

    def __init__(self, func_name: str, arg_name: str) -> None:
        """Initialize the ArgumentException.

        Args:
            func_name (str): The corresponding function name.
            arg_name (str): The corresponding argument name.
        """
        msg = f"Argument {arg_name} wasn't passed for the DAG/ExecNode created from function {func_name}"
        super().__init__(msg)


class TawaziTypeError(TawaziBaseException):
    """Raised when using Tawazi (Passing the wrong type of arguments etc.)."""

    pass


class TawaziUsageError(TawaziBaseException):
    """Raised when User miss uses Tawazi."""

    pass


def _raise_arg_exc(func_or_func_name: Union[str, Callable[[Any], Any]], arg_name: str) -> NoReturn:
    if isinstance(func_or_func_name, str):
        raise TawaziArgumentException(func_or_func_name, arg_name)

    raise TawaziArgumentException(func_or_func_name.__qualname__, arg_name)


class InvalidExecNodeCall(TawaziBaseException):
    """Raised when a ExecNode is called outside DAG definition (this will change in the future)."""

    pass
