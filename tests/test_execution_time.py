from copy import deepcopy
from time import sleep, time
from typing import Any, Tuple

from tawazi import Resource, dag, xn

from tests.helpers import UseDill

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


def test_threads_io_bound_computation_timing() -> None:
    @dag(max_threads_concurrency=2)
    def deps() -> None:
        a_ = a()
        b_ = b()
        c(a_, b_)

    t0 = time()
    deps()
    execution_time = time() - t0
    assert execution_time < 2.5 * T


def test_processes_io_bound_computation_timing() -> None:
    global a, b, c
    a = deepcopy(a)
    b = deepcopy(b)
    c = deepcopy(c)
    for xn_ in [a, b, c]:
        xn_.resource = Resource.process  # type: ignore[attr-defined]

    @dag(max_processes_concurrency=2)
    def deps() -> None:
        a_ = a()
        b_ = b()
        c(a_, b_)

    t0 = time()
    deps()
    execution_time = time() - t0
    assert execution_time < 2.5 * T


def test_threads_cpu_bound_computation_timing() -> None:
    @xn(resource=Resource.thread)
    def computation(n: int) -> int:
        return sum(range(n))

    n: int = 10**7
    # execution one time to cache the function?
    computation.__wrapped__(n)  # type: ignore[attr-defined]

    t0 = time()
    computation.__wrapped__(n)  # type: ignore[attr-defined]
    ct = time() - t0

    @dag(max_threads_concurrency=2)
    def pipe() -> Tuple[int, int]:
        return computation(n), computation(n)

    t0 = time()
    pipe()
    ct_pipe = time() - t0

    # ideally this would be a 2x CT, but we allow for some overhead
    assert ct_pipe >= 1.6 * ct


def test_processes_cpu_bound_computation_timing_1_worker() -> None:
    with UseDill():

        @xn(resource=Resource.process)
        def computation(n: int) -> int:
            return sum(range(n))

        n: int = 10**7
        # execution one time to cache the function?
        computation.__wrapped__(n)  # type: ignore[attr-defined]

        t0 = time()
        computation.__wrapped__(n)  # type: ignore[attr-defined]
        ct = time() - t0

        @dag(max_processes_concurrency=1)
        def pipe() -> Tuple[int, int]:
            return computation(n), computation(n)

        t0 = time()
        pipe()
        ct_pipe = time() - t0

        # ideally this would be a 2x CT, but we allow for some overhead
        assert ct_pipe > 1.6 * ct


def test_processes_cpu_bound_computation_timing_2_workers() -> None:
    with UseDill():

        @xn(resource=Resource.process)
        def computation(n: int) -> int:
            return sum(range(n))

        n: int = 10**7

        t0 = time()
        computation.__wrapped__(n)  # type: ignore[attr-defined]
        ct = time() - t0

        @dag(max_processes_concurrency=2)
        def pipe() -> Tuple[int, int]:
            return computation(n), computation(n)

        t0 = time()
        pipe()
        ct_pipe = time() - t0

        # ideally this would be a 2x CT, but we allow for some overhead
        assert ct_pipe < 1.4 * ct
