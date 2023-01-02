# type: ignore
from time import sleep, time

from tawazi import op, to_dag

"""integration test"""

T = 0.1


@op
def a():
    sleep(T)


@op
def b():
    sleep(T)


@op
def c(a, b):
    sleep(T)


@to_dag(max_concurrency=2)
def deps():
    a_ = a()
    b_ = b()
    c_ = c(a_, b_)


def test_timing():
    t0 = time()
    deps().execute()
    execution_time = time() - t0
    assert execution_time < 2.5 * T
