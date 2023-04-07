from copy import deepcopy
from typing import Any, Tuple, Union

import pytest
from tawazi import DAG, dag, xn


@xn
def a(i: int) -> int:
    return i + 1


@xn
def b(i: int) -> int:
    return i + 1


@xn
def c(i: int) -> int:
    return i + 1


@xn
def d(i: int) -> int:
    return i + 1


@xn
def e(i: int) -> int:
    return i + 1


@dag
def linear_pipe(i: int) -> int:
    a_ = a(i)
    b_ = b(a_)
    c_ = c(b_)
    return d(c_)


def test_basic_compose() -> None:
    composed_dag: DAG[[int], int] = linear_pipe.compose(a, b)
    assert linear_pipe(0) == 4
    assert composed_dag(0) == 1
    assert linear_pipe(0) == 4
    assert composed_dag(0) == 1


def test_full_pipe() -> None:
    composed_dag: DAG[[int], int] = linear_pipe.compose("linear_pipe>>>i", d)
    assert linear_pipe(0) == 4
    assert composed_dag(0) == 4


@xn
def unwanted_xn() -> int:
    return 42


@xn
def x(v: Any) -> int:
    return int(v)


@xn
def y(v: Any) -> str:
    return str(v)


@xn
def z(x: int, y: Union[str, int]) -> float:
    return float(x) + float(y)


@dag
def xyz_pipe() -> Tuple[int, float, int]:
    a = unwanted_xn()
    res = z(x(1), y(1))
    b = unwanted_xn()
    return a, res, b


def test_typing() -> None:
    composed_dag: DAG[[int, str], float] = xyz_pipe.compose([x, y], z)
    assert composed_dag(2, "4") == 6.0


@dag
def diamond_pipe(v: Union[int, str]) -> float:
    v1 = x(v)
    v2 = x(v1)
    v3 = y(v1)
    return z(v2, v3)


def test_input_insufficient_to_produce_output() -> None:
    with pytest.raises(
        ValueError, match="Either declare them as inputs or modify the requests outputs."
    ):
        _ = diamond_pipe.compose([], z)


def test_input_more_sufficient_to_produce_output() -> None:
    @dag
    def diamond_pipe(v: Union[int, str]) -> Tuple[float, int]:
        v1 = x(v)
        v2 = x(v1)
        v3 = y(v1)
        cst = x(123, twz_tag="cst")  # type: ignore[call-arg]
        return z(v2, v3), cst

    with pytest.warns(
        UserWarning,
        match="Input ExecNode x<<2>> is not used to produce any of the requested outputs.",
    ):
        sub = diamond_pipe.compose([x, "cst"], "x<<1>>")
        assert sub(2, 123) == 2


def test_input_more_sufficient_to_produce_empty_output() -> None:
    with pytest.warns(
        UserWarning, match="Input ExecNode x is not used to produce any of the requested outputs."
    ):
        sub = diamond_pipe.compose(x, [])
        assert sub(2) == ()


def test_duplicate_tag_in_inputs() -> None:
    @dag
    def diamond_pipe(v: Union[int, str]) -> float:
        v1 = x(v)
        v2 = x(v1, twz_tag="twinkle")  # type: ignore[call-arg]
        v3 = y(v1, twz_tag="twinkle")  # type: ignore[call-arg]
        return z(v2, v3)

    with pytest.raises(ValueError, match="Alias twinkle is not unique. It points to 2 ExecNodes: "):
        _ = diamond_pipe.compose("twinkle", "z")


def test_multiple_return_value() -> None:
    sub: DAG[[int], Tuple[int, int, str, float]] = diamond_pipe.compose(
        ["diamond_pipe>>>v"], ["x", "x<<1>>", "y", "z"]
    )
    assert sub(2) == (2, 2, "2", 4.0)


def test_input_successor_output() -> None:
    with pytest.raises(
        ValueError, match="Either declare them as inputs or modify the requests outputs."
    ):
        _ = diamond_pipe.compose(z, [x, y])


def test_input_successor_input() -> None:
    with pytest.raises(ValueError, match="this is ambiguous. Remove either one of them."):
        _ = linear_pipe.compose([a, b], c)


def test_inputs_outputs_overlapping() -> None:
    with pytest.raises(
        ValueError, match="Either declare them as inputs or modify the requests outputs."
    ):
        _ = linear_pipe.compose(a, a)


def test_inputs_empty_outputs_empty() -> None:
    dag_compose = linear_pipe.compose([], [])
    assert dag_compose() == ()


def test_outputs_depends_on_csts_subgraph() -> None:
    c_dag: DAG[[int], float] = xyz_pipe.compose([x], z)
    assert c_dag(2) == 3.0


def test_outputs_depends_on_csts_subgraph2() -> None:
    c_dag = xyz_pipe.compose([], z)
    assert c_dag() == 2.0


@dag
def diamond_pipe2(v: int, c: int = 10) -> float:
    v1 = v + c
    v2 = x(v1)
    v3 = y(v1)
    return z(v2, v3)


def test_outputs_depends_on_csts_subgraph_including_const_inputs() -> None:
    c_dag = diamond_pipe2.compose("diamond_pipe2>>>v", z)
    assert c_dag(2) == 24.0


setop_counter = 0


@xn(setup=True)
def setop() -> str:
    global setop_counter
    setop_counter += 1
    return "twinkle toes"


@dag
def diamond_pipe_setop(v: int) -> Tuple[str, int]:
    s = setop()
    v1 = y(s)
    return v1, x(v)


def test_setop() -> None:
    global setop_counter

    diamond_pipe_setop_ = deepcopy(diamond_pipe_setop)
    assert diamond_pipe_setop_(2) == ("twinkle toes", 2)
    assert setop_counter == 1
    assert diamond_pipe_setop_(2) == ("twinkle toes", 2)
    assert setop_counter == 1

    diamond_pipe_setop_ = deepcopy(diamond_pipe_setop)
    c_dag = diamond_pipe_setop_.compose("diamond_pipe_setop>>>v", [y, x])
    assert c_dag(2) == ("twinkle toes", 2)
    assert setop_counter == 2
    assert c_dag(2) == ("twinkle toes", 2)
    assert setop_counter == 2

    diamond_pipe_setop_ = deepcopy(diamond_pipe_setop)
    c_dag = diamond_pipe_setop_.compose("diamond_pipe_setop>>>v", x)
    assert c_dag(2) == 2
    assert setop_counter == 2
