import threading
from time import sleep, time
from typing import Tuple

import pytest
from tawazi import Resource, dag, xn

from tests.helpers import UseDill

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

    @dag(max_threads_concurrency=2)
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


def test_xn_in_process() -> None:
    with UseDill():

        @xn(resource=Resource.process)
        def xn1() -> int:
            return 1

        @dag
        def pipe() -> int:
            return xn1()

        assert pipe() == 1


def test_xns_in_mixed_resources() -> None:
    with UseDill():

        @xn(resource=Resource.process)
        def xn1() -> int:
            return 1

        @xn(resource=Resource.thread)
        def xn2() -> int:
            return 2

        @xn(resource=Resource.main_thread)
        def xn3() -> int:
            return 3

        @dag
        def pipe() -> Tuple[int, int, int]:
            return xn1(), xn2(), xn3()

        assert pipe() == (1, 2, 3)


def test_exec_node_in_process_tawazi_dill_node_not_set() -> None:
    with pytest.warns(
        UserWarning,
        match="You are trying to run a Node in a process, but TAWAZI_DILL_NODES is set to False. ",
    ):

        @xn(resource=Resource.process)
        def xn1() -> int:
            return 1
