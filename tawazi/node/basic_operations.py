"""Basic operators definitions."""

from typing import Any, Callable

from tawazi.config import Cfg
from tawazi.consts import RVXN, P

from .node import LazyExecNode


def _xn(func: Callable[P, RVXN]) -> LazyExecNode[P, RVXN]:
    return LazyExecNode(func, 0, Cfg.TAWAZI_IS_SEQUENTIAL, False, None, False, None)


# boolean operators definitions
# "rich comparison" methods definitions
@_xn
def _uxn_lt(a: Any, b: Any) -> bool:
    return a.__lt__(b)  # type: ignore[no-any-return]


@_xn
def _uxn_le(a: Any, b: Any) -> bool:
    return a.__le__(b)  # type: ignore[no-any-return]


@_xn
def _uxn_eq(a: Any, b: Any) -> bool:
    return a.__eq__(b)  # type: ignore[no-any-return]


@_xn
def _uxn_ne(a: Any, b: Any) -> bool:
    return a.__ne__(b)  # type: ignore[no-any-return]


@_xn
def _uxn_gt(a: Any, b: Any) -> bool:
    return a.__gt__(b)  # type: ignore[no-any-return]


@_xn
def _uxn_ge(a: Any, b: Any) -> bool:
    return a.__ge__(b)  # type: ignore[no-any-return]


# "numeric emulation" methods definitions
# https://docs.python.org/3/reference/datamodel.html#emulating-numeric-types

# binary operations
# automatically implement:
# 1. right hand operations (allowing float + UsageExecNode[float])
# 2. augmented arithmetic (+=, -=, etc.)@_xn


@_xn
def _uxn_add(a: Any, b: Any) -> Any:
    return a.__add__(b)


@_xn
def _uxn_sub(a: Any, b: Any) -> Any:
    return a.__sub__(b)


@_xn
def _uxn_mul(a: Any, b: Any) -> Any:
    return a.__mul__(b)


@_xn
def _uxn_matmul(a: Any, b: Any) -> Any:
    return a.__matmul__(b)


@_xn
def _uxn_truediv(a: Any, b: Any) -> Any:
    return a.__truediv__(b)


@_xn
def _uxn_floordiv(a: Any, b: Any) -> Any:
    return a.__floordiv__(b)


@_xn
def _uxn_mod(a: Any, b: Any) -> Any:
    return a.__mod__(b)


@_xn
def _uxn_divmod(a: Any, b: Any) -> Any:
    return a.__divmod__(b)


@_xn
def _uxn_pow(a: Any, b: Any) -> Any:
    return a.__pow__(b)


@_xn
def _uxn_lshift(a: Any, b: Any) -> Any:
    return a.__lshift__(b)


@_xn
def _uxn_rshift(a: Any, b: Any) -> Any:
    return a.__rshift__(b)


@_xn
def _uxn_and(a: Any, b: Any) -> Any:
    return a.__and__(b)


@_xn
def _uxn_xor(a: Any, b: Any) -> Any:
    return a.__xor__(b)


@_xn
def _uxn_or(a: Any, b: Any) -> Any:
    return a.__or__(b)
