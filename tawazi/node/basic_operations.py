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
