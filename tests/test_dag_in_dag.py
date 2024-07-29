from typing import Dict, List, Literal, Tuple

import pytest
from tawazi import dag, xn


@xn
def x1() -> Literal[1]:
    return 1


@xn
def x2(v: int) -> int:
    return v


def test_simple() -> None:
    @dag
    def d1() -> int:
        return x1() + x1()

    @dag
    def d2() -> int:
        return d1() + x1()

    assert d2() == 3


def test_pass_args() -> None:
    @dag
    def d1(v: int) -> int:
        return x1() + x2(v)

    @dag
    def d2() -> int:
        return d1(x1())

    assert d2() == 2


@dag
def d1(v: int, default: int = 2) -> int:
    return x1() + x2(v) + x2(default)


def test_pass_const_arg() -> None:
    @dag
    def d2() -> int:
        return d1(x1(), 2)

    assert d2() == 4


def test_pass_default_arg() -> None:
    @dag
    def d2() -> int:
        return d1(x1())

    assert d2() == 4


def test_return_tuple() -> None:
    @dag
    def d1() -> Tuple[int, int]:
        return x1(), x1()

    @dag
    def d2() -> int:
        a, b = d1()
        return a + b

    assert d2() == 2


def test_return_list() -> None:
    @dag
    def d1() -> List[int]:
        return [x1(), x1()]

    @dag
    def d2() -> int:
        a, b = d1()
        return a + b

    assert d2() == 2


def test_return_dict() -> None:
    @dag
    def d1() -> Dict[str, int]:
        return {"a": x1(), "b": x1()}

    @dag
    def d2() -> int:
        d = d1()
        return d["a"] + d["b"]

    assert d2() == 2


def test_return_none() -> None:
    @dag
    def d1() -> None:
        return None

    with pytest.raises(RuntimeError, match="SubDAG must have return values"):

        @dag
        def d2() -> None:
            return d1()
