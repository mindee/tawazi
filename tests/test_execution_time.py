from time import sleep, time
from typing import Any

from tawazi import dag, xn
from tawazi.consts import Resource

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


def test_timing_async_threaded() -> None:
    @xn(resource=Resource.async_thread)
    def sleep_sync_async_thread() -> str:
        sleep(T)
        return "twinkle"

    @dag(max_concurrency=2)
    def pipeline() -> str:
        a()
        b()
        return sleep_sync_async_thread()

    t0 = time()
    assert pipeline() == "twinkle"
    execution_time = time() - t0
    assert execution_time < 2.5 * T
