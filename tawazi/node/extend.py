"""Module to implement built-in operators on UsageExecNode."""

from typing import Any, Callable

from tawazi.config import cfg
from tawazi.consts import RVXN, P

from .node import LazyExecNode
from .uxn import UsageExecNode


def _xn(func: Callable[P, RVXN]) -> LazyExecNode[P, RVXN]:
    return LazyExecNode(
        exec_function=func,
        priority=0,
        is_sequential=cfg.TAWAZI_IS_SEQUENTIAL,
        debug=False,
        tag=None,
        setup=False,
        unpack_to=None,
        resource=cfg.TAWAZI_DEFAULT_RESOURCE,
    )


# boolean operators definitions
# "rich comparison" methods definitions
@_xn
def _lt(a: Any, b: Any) -> bool:
    return a < b  # type: ignore[no-any-return]


@_xn
def _le(a: Any, b: Any) -> bool:
    return a <= b  # type: ignore[no-any-return]


@_xn
def _eq(a: Any, b: Any) -> bool:
    return a == b  # type: ignore[no-any-return]


@_xn
def _ne(a: Any, b: Any) -> bool:
    return a != b  # type: ignore[no-any-return]


@_xn
def _gt(a: Any, b: Any) -> bool:
    return a > b  # type: ignore[no-any-return]


@_xn
def _ge(a: Any, b: Any) -> bool:
    return a >= b  # type: ignore[no-any-return]


# "numeric emulation" methods definitions
# https://docs.python.org/3/reference/datamodel.html#emulating-numeric-types

# binary operations
# automatically implement:
# 1. right hand operations (allowing float + UsageExecNode[float])
# 2. augmented arithmetic (+=, -=, etc.)@_xn


@_xn
def _add(a: Any, b: Any) -> Any:
    return a + b


@_xn
def _sub(a: Any, b: Any) -> Any:
    return a - b


@_xn
def _mul(a: Any, b: Any) -> Any:
    return a * b


@_xn
def _matmul(a: Any, b: Any) -> Any:
    return a @ b


@_xn
def _truediv(a: Any, b: Any) -> Any:
    return a / b


@_xn
def _floordiv(a: Any, b: Any) -> Any:
    return a // b


@_xn
def _mod(a: Any, b: Any) -> Any:
    return a % b


@_xn
def _divmod(a: Any, b: Any) -> Any:
    return divmod(a, b)


@_xn
def _pow(a: Any, b: Any) -> Any:
    return pow(a, b)


@_xn
def _lshift(a: Any, b: Any) -> Any:
    return a << b


@_xn
def _rshift(a: Any, b: Any) -> Any:
    return a >> b


@_xn
def _and(a: Any, b: Any) -> Any:
    return a & b


@_xn
def _xor(a: Any, b: Any) -> Any:
    return a ^ b


@_xn
def _or(a: Any, b: Any) -> Any:
    return a | b


# unary operations
@_xn
def _neg(a: Any) -> Any:
    return -a


@_xn
def _pos(a: Any) -> Any:
    return +a


@_xn
def _abs(a: Any) -> Any:
    return abs(a)


@_xn
def _invert(a: Any) -> Any:
    return ~a


# built ins
# when a value is used for indexing of another
# can't be implemented because should return appropriate type
# object.__index__(self)

# can't be implemented because should return appropriate type
# __complex__
# __int__
# __float__

# __round__
# __trunc__
# __floor__
# __ceil__

# Assign built-in operators to UsageExecNode


def reflected(operator: Callable[[Any, Any], Any]) -> Callable[[Any, Any], Any]:
    """Reflects an operator."""

    def inner_reflected(a: Any, b: Any) -> Any:
        return operator(b, a)

    return inner_reflected


# binary operations
setattr(UsageExecNode, "__lt__", _lt)  # noqa: B010
setattr(UsageExecNode, "__le__", _le)  # noqa: B010
setattr(UsageExecNode, "__eq__", _eq)  # noqa: B010
setattr(UsageExecNode, "__ne__", _ne)  # noqa: B010
setattr(UsageExecNode, "__gt__", _gt)  # noqa: B010
setattr(UsageExecNode, "__ge__", _ge)  # noqa: B010
setattr(UsageExecNode, "__add__", _add)  # noqa: B010
setattr(UsageExecNode, "__sub__", _sub)  # noqa: B010
setattr(UsageExecNode, "__mul__", _mul)  # noqa: B010
setattr(UsageExecNode, "__matmul__", _matmul)  # noqa: B010
setattr(UsageExecNode, "__truediv__", _truediv)  # noqa: B010
setattr(UsageExecNode, "__floordiv__", _floordiv)  # noqa: B010
setattr(UsageExecNode, "__mod__", _mod)  # noqa: B010
setattr(UsageExecNode, "__divmod__", _divmod)  # noqa: B010
setattr(UsageExecNode, "__pow__", _pow)  # noqa: B010
setattr(UsageExecNode, "__lshift__", _lshift)  # noqa: B010
setattr(UsageExecNode, "__rshift__", _rshift)  # noqa: B010
setattr(UsageExecNode, "__and__", _and)  # noqa: B010
setattr(UsageExecNode, "__xor__", _xor)  # noqa: B010
setattr(UsageExecNode, "__or__", _or)  # noqa: B010

# reflected binary operators
setattr(UsageExecNode, "__radd__", reflected(_add))  # noqa:B010
setattr(UsageExecNode, "__rsub__", reflected(_sub))  # noqa:B010
setattr(UsageExecNode, "__rmul__", reflected(_mul))  # noqa:B010
setattr(UsageExecNode, "__rmatmul__", reflected(_matmul))  # noqa:B010
setattr(UsageExecNode, "__rtruediv__", reflected(_truediv))  # noqa:B010
setattr(UsageExecNode, "__rfloordiv__", reflected(_floordiv))  # noqa:B010
setattr(UsageExecNode, "__rmod__", reflected(_mod))  # noqa:B010
setattr(UsageExecNode, "__rdivmod__", reflected(_divmod))  # noqa:B010
setattr(UsageExecNode, "__rpow__", reflected(_pow))  # noqa:B010
setattr(UsageExecNode, "__rlshift__", reflected(_lshift))  # noqa:B010
setattr(UsageExecNode, "__rrshift__", reflected(_rshift))  # noqa:B010
setattr(UsageExecNode, "__rand__", reflected(_and))  # noqa:B010
setattr(UsageExecNode, "__rxor__", reflected(_xor))  # noqa:B010
setattr(UsageExecNode, "__ror__", reflected(_or))  # noqa:B010

# unary operations
setattr(UsageExecNode, "__neg__", _neg)  # noqa: B010
setattr(UsageExecNode, "__pos__", _pos)  # noqa: B010
setattr(UsageExecNode, "__abs__", _abs)  # noqa: B010
setattr(UsageExecNode, "__invert__", _invert)  # noqa: B010
