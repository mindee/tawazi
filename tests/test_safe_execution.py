#  type: ignore
from time import sleep

import pytest

from tawazi import op, to_dag

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
a_op = op(a, priority=10)
b_op = op(b)
c_op = op(c)

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
        dag = dagger()
        dag.max_concurrency = 1
        dag.execute()
        assert pytest.safe_execution_val == "ABC", f"during {i}th iteration"


def test_safe_execution():
    pytest.safe_execution_val = ""
    dag = dagger()
    dag.safe_execute()
    assert pytest.safe_execution_val in ["ABC", "BAC"]
