#  type: ignore
from time import sleep

import pytest

from tawazi import op, to_dag

T = 0.1


def a():
    sleep(T)
    pytest.val += "A"
    return "A"


def b():
    sleep(T)
    pytest.val += "B"
    return "B"


def c(a, b):
    sleep(T)
    pytest.val += "C"
    return f"{a} + {b} = C"


def run_without_dag():
    a_ = a()
    b_ = b()
    c_ = c(a_, b_)


# apply the operation decorator
# make sure "a" runs first before "b"
a_op = op(a, priority=10)
b_op = op(b)
c_op = op(c)

# run in the dag interface
@to_dag
def dagger():
    a_ = a_op()
    b_ = b_op()
    c_ = c_op(a, b)


def test_normal_execution_without_dag():
    pytest.val = ""
    run_without_dag()
    assert pytest.val == "ABC"


def test_dag_execution():
    pytest.val = ""
    dag = dagger()
    dag.max_concurrency = 2
    dag.execute()
    assert pytest.val == "ABC"


def test_safe_execution():
    pytest.val = ""
    dag = dagger()
    dag.safe_execute()
    assert pytest.val in ["ABC", "BAC"]
