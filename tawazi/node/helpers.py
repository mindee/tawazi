"""Helpers for node subpackage."""
from typing import Any, Callable, Optional


def _validate_tuple(func: Callable[..., Any], unpack_to: int) -> Optional[bool]:
    """Validate tuple typing when upack_to is set.

    Args:
        func: the function to validate
        unpack_to: the number of elements in the unpacked results.

    Returns:
        Optional[bool]: True if the validation is successful, None if validation can not be conclusive.
    """
    r_type = func.__annotations__.get("return")
    # maybe typing is not provided!
    if not r_type:
        return None
    if isinstance(r_type, str):
        # eval is safe to use because the user is providing the typing and evaluating it locally
        r_type = eval(r_type, func.__globals__)  # noqa: PGH001  # nosec B307
    is_tuple = r_type.__origin__ is tuple
    if not is_tuple:
        return None
    args = r_type.__args__
    # maybe Tuple length is not specified!
    if args[-1] is ...:
        return None

    if len(args) == unpack_to:
        return True
    raise ValueError(
        f"unpack_to must be equal to the number of elements in the type of return ({r_type}) of the function {func}, provided {unpack_to}"
    )
