from time import sleep
from typing import Any

from tawazi import dag, xn

compound_priority_str = ""
T = 1e-3


@xn(priority=1)
def a() -> None:
    sleep(T)
    global compound_priority_str
    compound_priority_str += "a"


@xn(priority=1)
def b(a: Any) -> None:
    sleep(T)
    global compound_priority_str
    compound_priority_str += "b"


@xn(priority=1)
def c(a: Any) -> None:
    sleep(T)
    global compound_priority_str
    compound_priority_str += "c"


@xn(priority=1)
def d(b: Any) -> None:
    sleep(T)
    global compound_priority_str
    compound_priority_str += "d"


@xn(priority=1)
def e() -> None:
    sleep(T)
    global compound_priority_str
    compound_priority_str += "e"


@dag
def dependency_describer() -> None:
    _a = a()
    _b = b(_a)
    _c = c(_a)
    _d = d(_b)
    _e = e()


def test_compound_priority() -> None:
    dag = dependency_describer

    assert dag.node_dict_by_name["a"].compound_priority == 4
    assert dag.node_dict_by_name["b"].compound_priority == 2
    assert dag.node_dict_by_name["c"].compound_priority == 1
    assert dag.node_dict_by_name["d"].compound_priority == 1
    assert dag.node_dict_by_name["e"].compound_priority == 1


def test_compound_priority_execution() -> None:
    global compound_priority_str
    compound_priority_str = ""
    dependency_describer()

    assert compound_priority_str.startswith("ab")
    assert len(compound_priority_str) == 5
