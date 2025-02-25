from typing import Any, Union

import pytest
from tawazi import dag, xn
from tawazi.errors import TawaziUsageError

glb_third_argument = None
glb_fourth_argument = None


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
    _2 = f2(_1)
    _3 = f3(_1, _2)
    _4 = f4(_1)
    _5 = f5(_1, _2)
    _6 = f6(foo=_1, bar=_2)
    _7 = f7(_1, _2, foo=_1, bar=_2)
    _8 = f8(_1, _2, _3)


def test_xns_signatures() -> None:
    pipe()


def test_xns_interface() -> None:
    @xn
    def a() -> str:
        return "a"

    @xn
    def b(a: str) -> str:
        return "b"

    @xn
    def c(a: str) -> str:
        return "c"

    @xn
    def d(
        b: str, c: str, third_argument: Union[str, int] = 1234, fourth_argument: int = 6789
    ) -> str:
        global glb_third_argument, glb_fourth_argument
        glb_third_argument = third_argument
        glb_fourth_argument = fourth_argument
        return "d"

    @dag
    def my_custom_dag() -> None:
        vara = a()
        varb = b(vara)
        varc = c(vara)
        _vard = d(varb, c=varc, fourth_argument=1111)

    @xn
    def e() -> str:
        return "e"

    @dag
    def my_other_custom_dag() -> tuple[Any, Any]:
        vara = a()
        varb = b(vara)
        varc = c(vara)
        vard = d(varb, c=varc, fourth_argument=2222, third_argument="blabla")
        vare = e()
        return vard, vare

    my_custom_dag()
    assert glb_third_argument == 1234
    assert glb_fourth_argument == 1111
    my_custom_dag()
    assert glb_third_argument == 1234
    assert glb_fourth_argument == 1111

    my_other_custom_dag()
    assert glb_third_argument == "blabla"
    assert glb_fourth_argument == 2222
    my_other_custom_dag()
    assert glb_third_argument == "blabla"
    assert glb_fourth_argument == 2222

    my_custom_dag()
    assert glb_third_argument == 1234
    assert glb_fourth_argument == 1111


def test_call_directly_with_warning() -> None:
    from tawazi import cfg
    from tawazi.consts import XNOutsideDAGCall

    cfg.TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR = XNOutsideDAGCall.warning

    with pytest.warns(RuntimeWarning):
        assert f8(1, 2, 3, foo=4, bar=5) == 15
    with pytest.warns(RuntimeWarning):
        assert f4(1) == 4


def test_call_directly_with_error() -> None:
    from tawazi import cfg
    from tawazi.consts import XNOutsideDAGCall

    cfg.TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR = XNOutsideDAGCall.error

    with pytest.raises(TawaziUsageError):
        assert f8(1, 2, 3, foo=4, bar=5) == 15
    with pytest.raises(TawaziUsageError):
        assert f4(1) == 4


def test_call_directly_with_ignore() -> None:
    from tawazi import cfg
    from tawazi.consts import XNOutsideDAGCall

    cfg.TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR = XNOutsideDAGCall.ignore

    assert f8(1, 2, 3, foo=4, bar=5) == 15
    assert f4(1) == 4
