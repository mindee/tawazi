from typing import Any

import pytest
from tawazi import dag, xn
from tawazi.errors import TawaziUsageError


# tests different cases of @op decoration for Python functions
# 1. different signatures
@xn
def f1() -> int:
    return 1


@xn
def f2(f1: int) -> int:
    return 2


@xn
def f3(f1: int, f2: int) -> int:
    assert f1 == 1
    assert f2 == 2
    return 3


@xn
def f4(f1: int, cst: int = 0) -> int:
    # TODO: test with argument param and without argument param cst
    return 4 + cst


@xn
def f5(*args: Any) -> int:
    return sum(args)


@xn
def f6(**kwargs: Any) -> int:
    return sum(kwargs.values())


@xn
def f7(*args: Any, **kwargs: Any) -> int:
    return sum(args) + sum(kwargs.values())  # type: ignore[no-any-return]


@xn
def f8(f1: int, *args: Any, **kwargs: Any) -> int:
    return sum([f1, *args, *(kwargs.values())])


@dag
def pipe() -> None:
    _1 = f1()
    # import ipdb
    # ipdb.set_trace()
    _2 = f2(_1)
    _3 = f3(_1, _2)
    _4 = f4(_1)
    _5 = f5(_1, _2)
    _6 = f6(foo=_1, bar=_2)
    _7 = f7(_1, _2, foo=_1, bar=_2)
    _8 = f8(_1, _2, _3)


def test_ops_signatures() -> None:
    pipe()


def test_call_directly_with_warning() -> None:
    from tawazi import cfg
    from tawazi.consts import XNOutsideDAGCall

    cfg.TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR = XNOutsideDAGCall.warning

    with pytest.warns(RuntimeWarning):
        assert 15 == f8(1, 2, 3, foo=4, bar=5)
    with pytest.warns(RuntimeWarning):
        assert 4 == f4(1)


def test_call_directly_with_error() -> None:
    from tawazi import cfg
    from tawazi.consts import XNOutsideDAGCall

    cfg.TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR = XNOutsideDAGCall.error

    with pytest.raises(TawaziUsageError):
        assert 15 == f8(1, 2, 3, foo=4, bar=5)
    with pytest.raises(TawaziUsageError):
        assert 4 == f4(1)


def test_call_directly_with_ignore() -> None:
    from tawazi import cfg
    from tawazi.consts import XNOutsideDAGCall

    cfg.TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR = XNOutsideDAGCall.ignore

    with pytest.warns(None) as record:  # type: ignore[call-overload]
        assert 15 == f8(1, 2, 3, foo=4, bar=5)
    assert len(record) == 0
    with pytest.warns(None) as record:  # type: ignore[call-overload]
        assert 4 == f4(1)
    assert len(record) == 0
