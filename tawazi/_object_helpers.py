"""Pre-instantiated objects that can be used as helpers."""

from typing import Any, TypeVar, Union

from ._decorators import xn

T = TypeVar("T")
V = TypeVar("V")


@xn
def and_(a: T, b: V) -> Union[T, V]:
    """Equivalent of `and` wrapped in ExecNode."""
    return a and b


@xn
def or_(a: T, b: V) -> Union[T, V]:
    """Equivalent of `or` wrapped in ExecNode."""
    return a or b


@xn
def not_(a: Any) -> bool:
    """Equivalent of `not` wrapped in ExecNode."""
    return not a
