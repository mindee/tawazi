import warnings
from typing import Any

import pytest
from tawazi import dag, xn
from tawazi.errors import TawaziUsageError
from tawazi.node.node import ExecNode


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

    with warnings.catch_warnings():
        assert 15 == f8(1, 2, 3, foo=4, bar=5)
    with warnings.catch_warnings():
        assert 4 == f4(1)


def test_non_kwarg_decoration() -> None:
    with pytest.raises(TypeError, match="0 is not a callable. Did you use a non-keyword argument?"):

        @xn(0)  # type: ignore[call-overload]
        def bla() -> int:
            return 1


def test_wrong_tag_type() -> None:
    with pytest.raises(ValueError, match="tag should be of type"):

        @xn(tag=0)
        def bla() -> int:
            return 1


def test_wrong_priority_type() -> None:
    with pytest.raises(ValueError, match="priority must be an int"):

        @xn(priority="0")  # type: ignore[call-overload]
        def bla() -> int:
            return 1


def test_wrong_resource_type() -> None:
    with pytest.raises(ValueError, match="resource must be of type"):

        @xn(resource="main-thread")  # type: ignore[call-overload]
        def bla() -> int:
            return 1


def test_wrong_args_and_kwargs() -> None:
    with pytest.raises(ValueError, match="args must be of type UsageExecNode"):
        ExecNode(args=(1,))  # type: ignore[arg-type]


def test_wrong_kwargs() -> None:
    with pytest.raises(ValueError, match="kwargs must be of type UsageExecNode"):
        ExecNode(kwargs={1: 1})  # type: ignore[dict-item]


def test_wrong_unpack_type() -> None:
    with pytest.raises(ValueError, match="unpack_to must be a positive int or None"):

        @xn(unpack_to=1.0)  # type: ignore[call-overload]
        def bla() -> int:
            return 1


def test_negative_unpack_type() -> None:
    with pytest.raises(ValueError, match="unpack_to must be a positive int or None"):

        @xn(unpack_to=-1)
        def bla() -> int:
            return 1
