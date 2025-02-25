from typing import Any, Optional

import pytest
from tawazi import dag, xn
from typing_extensions import Literal


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
    def d1() -> tuple[int, int]:
        return x1(), x1()

    @dag
    def d2() -> int:
        a, b = d1()
        return a + b

    assert d2() == 2


def test_return_list() -> None:
    @dag
    def d1() -> list[int]:
        return [x1(), x1()]

    @dag
    def d2() -> int:
        a, b = d1()
        return a + b

    assert d2() == 2


def test_return_dict() -> None:
    @dag
    def d1() -> dict[str, int]:
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


def test_is_active() -> None:
    @xn
    def to_bool(v: Any) -> bool:
        return bool(v)

    @dag
    def d2(v: int) -> tuple[Optional[bool], Optional[int]]:
        res = d1(v, twz_active=v > 0)  # type: ignore[call-arg]
        b = to_bool(res)
        return b, res

    assert d2(1) == (True, 4)
    assert d2(0) == (False, None)


counter = 0


@xn
def incr_counter() -> None:
    global counter
    counter += 1


def test_is_active_deactivates_all_nodes_in_dag_in_dag() -> None:
    global counter
    counter = 0

    @dag
    def subdag(v: int) -> int:
        incr_counter()
        return v + 1

    @dag
    def my_dag(v: int, activate: bool) -> int:
        return subdag(v, twz_active=activate)  # type: ignore[call-arg]

    assert counter == 0
    assert my_dag(1, True) == 2
    assert counter == 1
    assert my_dag(1, False) is None
    assert counter == 1


def test_kwarg_in_xn() -> None:
    @dag
    def subdag() -> int:
        return x2(v=1)

    @dag
    def my_dag() -> int:
        return subdag()

    assert my_dag() == 1


@dag
def subdag_active(v: int, activate: bool) -> int:
    return x2(v=v, twz_active=activate)  # type: ignore[call-arg]


def test_active_in_subdag() -> None:
    @dag
    def my_dag(v: int, activate: bool) -> int:
        return subdag_active(v, activate)

    assert my_dag(1, True) == 1
    assert my_dag(1, False) is None
    assert my_dag(1, True) == 1
    assert my_dag(1, False) is None


def test_active_in_subdag_and_active_in_dag() -> None:
    with pytest.raises(RuntimeError, match="already has an activation"):

        @dag
        def my_dag(v: int, activate: bool, activate_subdag: bool) -> int:
            return subdag_active(v, activate, twz_active=activate_subdag)  # type: ignore[call-arg]


def test_recursive() -> None:
    with pytest.raises(NameError):
        with pytest.warns(UserWarning, match="Recursion is not supported for DAGs"):

            @dag
            def rec() -> None:
                return rec()


def test_active_argument_passing_argument() -> None:
    @dag
    def subdag(x: Any) -> Any:
        return x

    @dag
    def my_dag(x: Any) -> Any:
        return subdag(x, twz_active=x > 0)  # type: ignore[call-arg]

    assert my_dag(1) == 1
    assert my_dag(-1) is None
