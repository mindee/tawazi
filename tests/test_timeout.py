from copy import deepcopy
from time import sleep

import pytest
from tawazi import dag, xn
from tawazi._errors import TawaziTimeoutError

T = 0.1


@xn
def wait_n_t(n: int) -> None:
    sleep(n * T)


def test_simple_timeout() -> None:
    wait_n_t_ = deepcopy(wait_n_t)
    wait_n_t_.timeout = 1.5 * T

    @dag
    def d() -> None:
        wait_n_t_(1)
        wait_n_t_(2)

    with pytest.raises(TawaziTimeoutError):
        d()


def test_timeout_sequential_xn() -> None:
    wait_n_t_ = deepcopy(wait_n_t)
    wait_n_t_.is_sequential = True
    wait_n_t_.timeout = 1.5 * T

    @dag
    def d() -> None:
        wait_n_t_(1)
        wait_n_t_(2)

    with pytest.raises(TawaziTimeoutError):
        d()


def test_timeout_parallel_xn() -> None:
    wait_n_t_ = deepcopy(wait_n_t)
    wait_n_t_.timeout = 1.5 * T

    @dag(max_concurrency=2)
    def d() -> None:
        wait_n_t_(1)
        wait_n_t_(1)

    d()


def test_timeout_parallel_xn_fail() -> None:
    wait_n_t_ = deepcopy(wait_n_t)
    wait_n_t_.timeout = 1.5 * T

    @dag(max_concurrency=2)
    def d() -> None:
        wait_n_t_(1)
        wait_n_t_(1)
        wait_n_t_(2)

    with pytest.raises(TawaziTimeoutError):
        d()


# def test_timeout_cpu_bound_task() -> None:
#     @xn(timeout=0.1)
#     def cpu_bound()->int:
#         s = 0
#         for i in range(10**7):
#             s += i
#         return s
#         # return sum(range(10**8))

#     @dag
#     def d()->int:
#         return cpu_bound()

#     with pytest.raises(TawaziTimeoutError):
#         d()
#     print("I ended")

# test_timeout_cpu_bound_task()
