from time import sleep
from typing import Any

from tawazi import dag, xn

T = 0.01
priority_sequential_comp_str = ""


@xn
def a(*__args: Any) -> None:
    sleep(T)
    global priority_sequential_comp_str
    priority_sequential_comp_str += "a"


@xn
def b(*__args: Any) -> None:
    sleep(T)
    global priority_sequential_comp_str
    priority_sequential_comp_str += "b"


@xn
def c(*__args: Any) -> None:
    sleep(T)
    global priority_sequential_comp_str
    priority_sequential_comp_str += "c"


@xn
def d(*__args: Any) -> None:
    sleep(T)
    global priority_sequential_comp_str
    priority_sequential_comp_str += "d"


@xn
def e(*__args: Any) -> None:
    sleep(T)
    global priority_sequential_comp_str
    priority_sequential_comp_str += "e"


@dag
def my_dag() -> None:
    _a = a()
    _b = b(_a)
    _c = c(_b)
    _d = d(_a)


@dag
def my_seq_dag() -> None:
    _a = a()
    _b = b(_a)
    _c = c(_a)
    _d = d(_b)
    _e = e(_a)


def test_priority() -> None:
    global priority_sequential_comp_str
    conf = {
        "a": {"priority": 1, "is_sequential": False},
        "b": {"priority": 2, "is_sequential": False},
        "c": {"priority": 2, "is_sequential": False},
        "d": {"priority": 1, "is_sequential": False},
    }
    my_dag.config_from_dict({"nodes": conf})

    for _i in range(100):
        priority_sequential_comp_str = ""
        my_dag()
        assert priority_sequential_comp_str == "abcd", f"during {_i}th iteration"


def test_sequentiality() -> None:
    global priority_sequential_comp_str
    conf = {
        "a": {"is_sequential": False},
        "b": {"priority": 2, "is_sequential": False},
        "c": {"priority": 2, "is_sequential": False},
        "d": {"priority": 1, "is_sequential": False},
        "e": {"priority": 1, "is_sequential": True},
    }
    my_seq_dag.config_from_dict({"nodes": conf})
    for _i in range(100):
        # Sequentiality test
        priority_sequential_comp_str = ""
        my_seq_dag()

        ind_a = priority_sequential_comp_str.index("a")
        ind_b = priority_sequential_comp_str.index("b")
        ind_c = priority_sequential_comp_str.index("c")
        ind_d = priority_sequential_comp_str.index("d")
        ind_e = priority_sequential_comp_str.index("e")

        assert ind_e > ind_b, f"during {_i}th iteration"
        assert ind_d > ind_b, f"during {_i}th iteration"
        assert ind_b > ind_a, f"during {_i}th iteration"
        assert ind_c > ind_a, f"during {_i}th iteration"


def test_sequentiality_awaits_for_others_to_finish() -> None:
    # this test validates that when an is_sequential node launches, all other tests aren't run in parallel until it finishes
    global priority_sequential_comp_str
    priority_sequential_comp_str = "done"

    @xn(is_sequential=True)
    def a() -> bool:
        global priority_sequential_comp_str
        return priority_sequential_comp_str == "done"

    @xn
    def fill() -> None:
        global priority_sequential_comp_str
        priority_sequential_comp_str = ""
        sleep(T)
        priority_sequential_comp_str = "done"

    @dag(max_concurrency=10)
    def d() -> bool:
        for _i in range(100):
            fill()
        return a()

    assert d() is True
