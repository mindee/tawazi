"""Module for custom errors raised by Tawazi."""


class TawaziError(Exception):
    """BaseException of Tawazi from which all other exceptions inherit."""

    pass


class TawaziArgumentError(TawaziError):
    """Raised when using Tawazi (Passing the wrong number/type of arguments etc.)."""

    def __init__(self, func_name: str, arg_name: str) -> None:
        """Initialize the ArgumentException.

        Args:
            func_name (str): The corresponding function name.
            arg_name (str): The corresponding argument name.
        """
        msg = f"Argument {arg_name} wasn't passed for the DAG/ExecNode created from function {func_name}"
        super().__init__(msg)


class TawaziTypeError(TawaziError):
    """Raised when using Tawazi (Passing the wrong type of arguments etc.)."""

    pass


class TawaziUsageError(TawaziError):
    """Raised when User miss uses Tawazi."""

    pass


class InvalidExecNodeCallError(TawaziError):
    """Raised when a ExecNode is called outside DAG definition (this will change in the future)."""

    pass
