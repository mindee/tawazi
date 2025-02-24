from logging import Logger
from typing import Any

import pytest
from tawazi import dag, xn
from tawazi.errors import TawaziUsageError
from tawazi.node import ExecNode

logger = Logger(name="mylogger", level="ERROR")


@xn(setup=True)
def setop(bla: int = 123) -> int:
    return bla + 1


@xn(debug=True)
def my_debug_node(in1: Any, in2: Any, in3: Any) -> None:
    logger.debug(in1, in2, in3)


@xn
def xn1(in1: int) -> int:
    return in1 + 1


@xn
def xn2(in1: int) -> int:
    return in1 + 2


@xn
def xn3(in1: int, in2: int) -> int:
    return in1 + in2


@dag
def pipe(in1: int, in2: int) -> tuple[int, int, int]:
    r1 = xn1(in1)
    r2 = xn2(in2)
    r3 = xn3(r1, r2)
    return r1, r2, r3


@dag
def pipe_with_debug_and_setup(in1: int, in2: int) -> tuple[int, int, int, int]:
    r1 = xn1(in1)
    r2 = xn2(in2)
    r3 = xn3(r1, r2)
    setop_r = setop()
    my_debug_node(r1, r2, r3)
    return r1, r2, r3, setop_r


@pytest.fixture
def executor() -> Any:
    return pipe.executor()


@pytest.fixture
def sub_executor() -> Any:
    return pipe.executor(target_nodes=["xn1", "xn2"])


def test_run_whole_dag_executor(executor: Any) -> None:
    r1, r2, r3 = executor(1, 2)
    assert (r1, r2, r3) == (2, 4, 6)


def test_run_dag_executor_multiple_times(executor: Any) -> None:
    _ = executor(1, 2)

    with pytest.raises(TawaziUsageError):
        # object has already been executed
        _ = executor(3, 4)


def test_run_sub_dag_executor(sub_executor: Any) -> None:
    r1, r2, r3 = sub_executor(1, 2)
    assert (r1, r2, r3) == (2, 4, None)


def test_scheduled_nodes(sub_executor: Any) -> None:
    scheduled_nodes = set(sub_executor.graph.nodes)
    assert {"xn1", "xn2"}.issubset(scheduled_nodes)
    assert "xn3" not in scheduled_nodes


def test_executed(executor: Any) -> None:
    assert executor(1, 2) == (2, 4, 6)

    with pytest.raises(TawaziUsageError):
        assert executor(3, 4) == (4, 6, 10)


@pytest.mark.parametrize("target_nodes", [None, [setop], [my_debug_node]])
def test_executed_with_setup_nodes(target_nodes: list[ExecNode]) -> None:
    executor = pipe_with_debug_and_setup.executor(target_nodes=target_nodes)
    executor(1, 2)
    assert executor.executed
