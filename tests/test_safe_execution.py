from copy import deepcopy
from time import sleep
from typing import Tuple

from tawazi import dag, xn

T = 0.01
safe_execution_val = ""
safe_execution_op_cst_has_run = False
safe_execution_c = 0


def a() -> str:
    sleep(T)
    global safe_execution_val
    safe_execution_val += "A"
    return "A"


def b() -> str:
    sleep(T)
    global safe_execution_val
    safe_execution_val += "B"
    return "B"


def c(a: str, b: str) -> str:
    sleep(T)
    global safe_execution_val
    safe_execution_val += "C"
    return f"{a} + {b} = C"


def run_without_dag() -> None:
    a_ = a()
    b_ = b()
    c(a_, b_)


# apply the operation decorator
# make sure "a" runs first before "b"
a_op = xn(a, priority=10)
b_op = xn(b)
c_op = xn(c)


# run in the dag interface
@dag
def dagger() -> None:
    a_ = a_op()
    b_ = b_op()
    c_op(a_, b_)


def test_normal_execution_without_dag() -> None:
    global safe_execution_val
    safe_execution_val = ""
    run_without_dag()
    assert safe_execution_val == "ABC"


def test_dag_execution() -> None:
    global safe_execution_val
    for i in range(10):
        safe_execution_val = ""
        dagger.max_concurrency = 1
        dagger()
        assert safe_execution_val == "ABC", f"during {i}th iteration"


def test_safe_execution() -> None:
    global safe_execution_val
    safe_execution_val = ""
    dagger._safe_execute()
    assert safe_execution_val in ["ABC", "BAC"]


@xn
def op1(in1: int) -> int:
    return in1 + 1


@xn
def op_cst(in1: int = 1) -> int:
    global safe_execution_op_cst_has_run
    safe_execution_op_cst_has_run = True
    return in1 + 2


@xn(setup=True)
def setop(in1: int) -> int:
    global safe_execution_c
    safe_execution_c += 1
    return in1 + 3


@dag
def pipe() -> Tuple[int, int]:
    out_setop = setop(1)
    out1 = op1(out_setop)
    return out1, op_cst()


def test_normal_execution_with_setup() -> None:
    global safe_execution_c
    pipe_ = deepcopy(pipe)
    safe_execution_c = 0
    assert pipe_() == (5, 3)
    assert safe_execution_c == 1
    assert pipe_() == (5, 3)
    assert safe_execution_c == 1


def test_safe_execution_with_setup() -> None:
    global safe_execution_c
    pipe_ = deepcopy(pipe)
    safe_execution_c = 0
    assert pipe_._safe_execute() == (5, 3)
    assert safe_execution_c == 1
    assert pipe_._safe_execute() == (5, 3)
    assert safe_execution_c == 1


def test_subgraph_with_safe_execution_with_setup() -> None:
    global safe_execution_op_cst_has_run, safe_execution_c
    pipe_ = deepcopy(pipe)
    safe_execution_c = 0
    safe_execution_op_cst_has_run = False
    assert pipe_._safe_execute(target_nodes=["op1"]) == (5, None)
    assert safe_execution_c == 1
    assert safe_execution_op_cst_has_run is False

    assert pipe_._safe_execute(target_nodes=["op1"]) == (5, None)
    assert safe_execution_c == 1
    assert safe_execution_op_cst_has_run is False
