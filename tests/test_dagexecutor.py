import threading
from logging import Logger
from typing import Any, List, Tuple

import pytest
from tawazi import DAGExecution, ErrorStrategy, dag, xn
from tawazi.errors import TawaziUsageError
from tawazi.node import ExecNode

logger = Logger(name="mylogger", level="ERROR")


base_thread_name = "twinkle_toes"


@xn
def thread_naming_xn() -> None:
    assert threading.current_thread().name.startswith(base_thread_name)


@dag(behavior=ErrorStrategy.strict)
def thread_naming_dag() -> None:
    thread_naming_xn()


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
def pipe(in1: int, in2: int) -> Tuple[int, int, int]:
    r1 = xn1(in1)
    r2 = xn2(in2)
    r3 = xn3(r1, r2)
    return r1, r2, r3


@dag
def pipe_with_debug_and_setup(in1: int, in2: int) -> Tuple[int, int, int, int]:
    r1 = xn1(in1)
    r2 = xn2(in2)
    r3 = xn3(r1, r2)
    setop_r = setop()
    my_debug_node(r1, r2, r3)
    return r1, r2, r3, setop_r


@pytest.fixture
def executor() -> DAGExecution[[int, int], Tuple[int, int, int]]:
    return pipe.executor()


@pytest.fixture
def sub_executor() -> DAGExecution[[int, int], Tuple[int, int, int]]:
    return pipe.executor(target_nodes=["xn1", "xn2"])


def test_run_whole_dag_executor(executor: DAGExecution[[int, int], Tuple[int, int, int]]) -> None:
    r1, r2, r3 = executor(1, 2)

    assert (r1, r2, r3) == (2, 4, 6)
    assert len(executor.results) == 5


def test_run_dag_executor_multiple_times(
    executor: DAGExecution[[int, int], Tuple[int, int, int]]
) -> None:
    _ = executor(1, 2)

    with pytest.raises(TawaziUsageError):
        # object has already been executed
        _ = executor(3, 4)


def test_run_sub_dag_executor(sub_executor: DAGExecution[[int, int], Tuple[int, int, int]]) -> None:
    r1, r2, r3 = sub_executor(1, 2)
    assert (r1, r2, r3) == (2, 4, None)  # type: ignore[comparison-overlap]


def test_working_thread_naming() -> None:
    thread_naming_dag.executor(call_id=base_thread_name)()


def test_failing_thread_naming() -> None:
    with pytest.raises(AssertionError):
        thread_naming_dag.executor(call_id="tough")()


def test_scheduled_nodes(sub_executor: DAGExecution[[int, int], Tuple[int, int, int]]) -> None:
    scheduled_nodes = set(sub_executor.graph.nodes)
    assert {"xn1", "xn2"}.issubset(scheduled_nodes) and "xn3" not in scheduled_nodes


def test_executed(executor: DAGExecution[[int, int], Tuple[int, int, int]]) -> None:
    assert executor(1, 2) == (2, 4, 6)

    with pytest.raises(TawaziUsageError):
        assert executor(3, 4) == (4, 6, 10)


@pytest.mark.parametrize("target_nodes", [None, [setop], [my_debug_node]])
def test_executed_with_setup_nodes(target_nodes: List[ExecNode]) -> None:
    executor = pipe_with_debug_and_setup.executor(target_nodes=target_nodes)
    executor(1, 2)
    assert executor.executed
