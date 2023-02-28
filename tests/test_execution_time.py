from time import sleep, time
from typing import Any

from tawazi import dag, xn

T = 0.1


@xn
def a() -> None:
    sleep(T)


@xn
def b() -> None:
    sleep(T)


@xn
def c(a: Any, b: Any) -> None:
    sleep(T)


@dag(max_concurrency=2)
def deps() -> None:
    a_ = a()
    b_ = b()
    c(a_, b_)


def test_timing() -> None:
    t0 = time()
    deps()
    execution_time = time() - t0
    assert execution_time < 2.5 * T
