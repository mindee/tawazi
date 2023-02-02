# type: ignore
from time import sleep, time

from tawazi import to_dag, xnode

"""integration test"""

T = 0.1


@xnode
def a():
    sleep(T)


@xnode
def b():
    sleep(T)


@xnode
def c(a, b):
    sleep(T)


@to_dag(max_concurrency=2)
def deps():
    a_ = a()
    b_ = b()
    c_ = c(a_, b_)


def test_timing():
    t0 = time()
    deps()
    execution_time = time() - t0
    assert execution_time < 2.5 * T
