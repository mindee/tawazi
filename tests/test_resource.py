import threading
from time import sleep, time
from typing import Tuple

from tawazi import Resource, dag, xn

T = 0.1


def test_main_thread_resource_thread_name() -> None:
    main_thread_name = threading.current_thread().name

    @xn(resource=Resource.main_thread)
    def xn1() -> int:
        assert threading.current_thread().name == main_thread_name
        return 1

    @xn(resource=Resource.thread)
    def xn2() -> int:
        assert threading.current_thread().name != main_thread_name
        return 2

    @dag
    def pipe() -> Tuple[int, int]:
        return xn1(), xn2()

    assert pipe() == (1, 2)


def test_main_thread_resource_computation_time() -> None:
    main_thread_name = threading.current_thread().name

    @xn(resource=Resource.main_thread, priority=1)
    def xn1() -> int:
        assert threading.current_thread().name == main_thread_name
        sleep(T)
        return 1

    @xn(resource=Resource.thread, priority=2)
    def xn2() -> int:
        assert threading.current_thread().name != main_thread_name
        sleep(T)
        return 2

    @dag(max_concurrency=2)
    def pipe() -> Tuple[int, int]:
        return xn1(), xn2()

    t0 = time()
    r = pipe()
    t_exec = time() - t0
    assert t_exec < 1.5 * T
    assert r == (1, 2)


def test_main_thread_sequential_exec_node() -> None:
    @xn(resource=Resource.main_thread, is_sequential=True)
    def xn1() -> int:
        return 1

    @dag
    def pipe() -> int:
        return xn1()

    assert pipe() == 1
