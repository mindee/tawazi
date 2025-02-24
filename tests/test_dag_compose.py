import asyncio
from copy import deepcopy
from typing import Any, Optional, Union

import pytest
from tawazi import DAG, AsyncDAG, dag, xn


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
    composed_dag: DAG[[int], int] = linear_pipe.compose("twinkle", a, b)  # type: ignore[assignment]
    assert linear_pipe(0) == 4
    assert composed_dag(0) == 1
    assert linear_pipe(0) == 4
    assert composed_dag(0) == 1


@pytest.mark.parametrize(
    "force_async,dag_class,expected_class",
    [
        (None, DAG, DAG),
        (None, AsyncDAG, AsyncDAG),
        (True, DAG, AsyncDAG),
        (False, DAG, DAG),
        (True, AsyncDAG, AsyncDAG),
        (False, AsyncDAG, DAG),
    ],
)
def test_compose_different_combinations(
    force_async: Optional[bool],
    dag_class: Union[DAG[Any, Any], AsyncDAG[Any, Any]],
    expected_class: Union[DAG[Any, Any], AsyncDAG[Any, Any]],
) -> None:
    my_linear_pipe = deepcopy(linear_pipe)
    my_linear_pipe.__class__ = dag_class  # type: ignore[assignment]
    composed_linear_pipe = my_linear_pipe.compose("twinkle", a, b, is_async=force_async)
    assert isinstance(composed_linear_pipe, expected_class)  # type: ignore[arg-type]
    if expected_class == DAG:
        assert composed_linear_pipe(0) == 1
    else:
        assert asyncio.run(composed_linear_pipe(0)) == 1  # type: ignore[arg-type]


def test_full_pipe() -> None:
    composed_dag: DAG[[int], int] = linear_pipe.compose("twinkle", "linear_pipe>!>i", d)  # type: ignore[assignment]
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
def xyz_pipe() -> tuple[int, float, int]:
    a = unwanted_xn()
    res = z(x(1), y(1))
    b = unwanted_xn()
    return a, res, b


def test_typing() -> None:
    composed_dag: DAG[[int, str], float] = xyz_pipe.compose("twinkle", [x, y], z)  # type: ignore[assignment]
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
        _ = diamond_pipe.compose("twinkle", [], z)


def test_input_more_sufficient_to_produce_output() -> None:
    @dag
    def diamond_pipe(v: Union[int, str]) -> tuple[float, int]:
        v1 = x(v)
        v2 = x(v1)
        v3 = y(v1)
        cst = x(123, twz_tag="cst")  # type: ignore[call-arg]
        return z(v2, v3), cst

    with pytest.warns(
        UserWarning,
        match="Input ExecNode x<<2>> is not used to produce any of the requested outputs.",
    ):
        sub: DAG[[int, int], int] = diamond_pipe.compose("twinkle", [x, "cst"], "x<<1>>")  # type: ignore[assignment]
        assert sub(2, 123) == 2


def test_input_more_sufficient_to_produce_empty_output() -> None:
    with pytest.warns(
        UserWarning, match="Input ExecNode x is not used to produce any of the requested outputs."
    ):
        sub: DAG[[int], tuple[()]] = diamond_pipe.compose("twinkle", x, [])  # type: ignore[assignment]
        assert sub(2) == ()


def test_duplicate_tag_in_inputs() -> None:
    @dag
    def diamond_pipe(v: Union[int, str]) -> float:
        v1 = x(v)
        v2 = x(v1, twz_tag="twinkle")  # type: ignore[call-arg]
        v3 = y(v1, twz_tag="twinkle")  # type: ignore[call-arg]
        return z(v2, v3)

    with pytest.raises(ValueError, match="Alias twinkle is not unique. It points to 2 ExecNodes: "):
        _ = diamond_pipe.compose("mycomposed", "twinkle", "z")


def test_multiple_return_value() -> None:
    sub: DAG[[int], tuple[int, int, str, float]] = diamond_pipe.compose(
        "twinkle", ["diamond_pipe>!>v"], ["x", "x<<1>>", "y", "z"]  # type: ignore[assignment]
    )
    assert sub(2) == (2, 2, "2", 4.0)


def test_input_successor_output() -> None:
    with pytest.raises(
        ValueError, match="Either declare them as inputs or modify the requests outputs."
    ):
        with pytest.warns(
            UserWarning, match="is not used to produce any of the requested outputs."
        ):
            _ = diamond_pipe.compose("twinkle", z, [x, y])


def test_input_successor_input() -> None:
    with pytest.raises(ValueError, match="this is ambiguous. Remove either one of them."):
        _ = linear_pipe.compose("twinkle", [a, b], c)


def test_inputs_outputs_overlapping() -> None:
    with pytest.raises(
        ValueError, match="Either declare them as inputs or modify the requests outputs."
    ):
        with pytest.warns(
            UserWarning, match="is not used to produce any of the requested outputs."
        ):
            _ = linear_pipe.compose("twinkle", a, a)


def test_inputs_empty_outputs_empty() -> None:
    dag_compose: DAG[[], tuple[()]] = linear_pipe.compose("twinkle", [], [])  # type: ignore[assignment]
    assert dag_compose() == ()


def test_outputs_depends_on_csts_subgraph() -> None:
    c_dag: DAG[[int], float] = xyz_pipe.compose("twinkle", [x], z)  # type: ignore[assignment]
    assert c_dag(2) == 3.0


def test_outputs_depends_on_csts_subgraph2() -> None:
    c_dag: DAG[[], float] = xyz_pipe.compose("twinkle", [], z)  # type: ignore[assignment]
    assert c_dag() == 2.0


@dag
def diamond_pipe2(v: int, c: int = 10) -> float:
    v1 = v + c
    v2 = x(v1)
    v3 = y(v1)
    return z(v2, v3)


def test_outputs_depends_on_csts_subgraph_including_const_inputs() -> None:
    c_dag = diamond_pipe2.compose("twinkle", "diamond_pipe2>!>v", z)
    assert c_dag(2) == 24.0


setop_counter = 0


@xn(setup=True)
def setop() -> str:
    global setop_counter
    setop_counter += 1
    return "twinkle toes"


@dag
def diamond_pipe_setop(v: int) -> tuple[str, int]:
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
    c_dag = diamond_pipe_setop_.compose("twinkle", "diamond_pipe_setop>!>v", [y, x])
    assert c_dag(2) == ("twinkle toes", 2)
    assert setop_counter == 2
    assert c_dag(2) == ("twinkle toes", 2)
    assert setop_counter == 2

    diamond_pipe_setop_ = deepcopy(diamond_pipe_setop)
    d_dag: DAG[[int], int] = diamond_pipe_setop_.compose("twinkle", "diamond_pipe_setop>!>v", x)  # type: ignore[assignment]
    assert d_dag(2) == 2
    assert setop_counter == 2


def test_kwargs_in_xn_composed() -> None:
    @dag
    def dag_with_default_arg() -> int:
        return x(v=1)

    d: DAG[..., tuple[int]] = dag_with_default_arg.compose("twinkle", [], [x])  # type: ignore[assignment]
    assert d() == (1,)


@dag
def complex_dag(a1: int, a2: int, a3: int) -> tuple[int, int, int]:
    return a1 + a2 + a3, a1 - a2 - a3, a1 * a2 * a3


def test_provide_ellipsis_as_input() -> None:
    composed = complex_dag.compose("twinkle", ..., ["_add<<1>>", "_sub<<1>>", "_mul<<1>>"])  # type: ignore[arg-type]
    assert composed(1, 2, 3) == (6, -4, 6)
