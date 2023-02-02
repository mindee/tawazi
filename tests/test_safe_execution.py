#  type: ignore
from copy import deepcopy
from time import sleep

import pytest

from tawazi import to_dag, xnode

"""integration tests"""

T = 0.01


def a():
    sleep(T)
    pytest.safe_execution_val += "A"
    return "A"


def b():
    sleep(T)
    pytest.safe_execution_val += "B"
    return "B"


def c(a, b):
    sleep(T)
    pytest.safe_execution_val += "C"
    return f"{a} + {b} = C"


def run_without_dag():
    a_ = a()
    b_ = b()
    c_ = c(a_, b_)


# apply the operation decorator
# make sure "a" runs first before "b"
a_op = xnode(a, priority=10)
b_op = xnode(b)
c_op = xnode(c)

# run in the dag interface
@to_dag
def dagger():
    a_ = a_op()
    b_ = b_op()
    c_ = c_op(a_, b_)


def test_normal_execution_without_dag():
    pytest.safe_execution_val = ""
    run_without_dag()
    assert pytest.safe_execution_val == "ABC"


def test_dag_execution():
    for i in range(10):
        pytest.safe_execution_val = ""
        dagger.max_concurrency = 1
        dagger()
        assert pytest.safe_execution_val == "ABC", f"during {i}th iteration"


def test_safe_execution():
    pytest.safe_execution_val = ""
    dagger.safe_execute()
    assert pytest.safe_execution_val in ["ABC", "BAC"]


@xnode
def op1(in1):
    return in1 + 1


@xnode
def op_cst(in1=1):
    pytest.safe_execution_op_cst_has_run = True
    return in1 + 2


@xnode(setup=True)
def setop(in1):
    pytest.safe_execution_c += 1
    return in1 + 3


# TODO: this should raise a warning!! because passing in an argument to the setup nodes!!
@to_dag
def pipe(in1):
    out_setop = setop(in1)
    out1 = op1(out_setop)
    return out1, op_cst()


def test_normal_execution_with_setup():
    pipe_ = deepcopy(pipe)
    pytest.safe_execution_c = 0
    assert pipe_(1) == (5, 3)
    assert pytest.safe_execution_c == 1
    assert pipe_(2) == (5, 3)
    assert pytest.safe_execution_c == 1


def test_safe_execution_with_setup():
    pipe_ = deepcopy(pipe)
    pytest.safe_execution_c = 0
    assert pipe_.safe_execute(1) == (5, 3)
    assert pytest.safe_execution_c == 1
    assert pipe_.safe_execute(2) == (5, 3)
    assert pytest.safe_execution_c == 1


def test_subgraph_with_safe_execution_with_setup():
    pipe_ = deepcopy(pipe)
    pytest.safe_execution_c = 0
    pytest.safe_execution_op_cst_has_run = False
    assert pipe_.safe_execute(1, twz_nodes=["op1"]) == (5, None)
    assert pytest.safe_execution_c == 1
    assert pytest.safe_execution_op_cst_has_run == False

    assert pipe_.safe_execute(2, twz_nodes=["op1"]) == (5, None)
    assert pytest.safe_execution_c == 1
    assert pytest.safe_execution_op_cst_has_run == False
