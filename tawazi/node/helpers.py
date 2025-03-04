"""Helpers for node subpackage."""

from typing import Any, Callable, Optional, Union

from tawazi._helpers import ordinal
from tawazi.consts import USE_SEP_END, USE_SEP_START, Identifier


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
        r_type = eval(r_type, func.__globals__)  # noqa: PGH001,S307 # nosec B307
    if not hasattr(r_type, "__origin__"):
        return None
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


def make_suffix(name_or_order: Union[int, str]) -> str:
    """Create the suffix of the id of ExecNode.

    Args:
        name_or_order (int | str): The name of the argument or its order of usage in the invocation of the function
    """
    if isinstance(name_or_order, int):
        return f"{ordinal(name_or_order)} argument"
    return name_or_order


def _lazy_xn_id(base_id: Identifier, count_usages: int) -> Identifier:
    if count_usages > 0:
        return f"{base_id}{USE_SEP_START}{count_usages}{USE_SEP_END}"

    return base_id
