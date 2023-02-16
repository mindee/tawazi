#  type: ignore
from time import sleep

import pytest

from tawazi import dag, xn

"""Internal Unit Test"""

pytest.compound_priority_str: str = ""
T = 1e-3


@xn(priority=1)
def a():
    sleep(T)
    pytest.compound_priority_str += "a"


@xn(priority=1)
def b(a):
    sleep(T)
    pytest.compound_priority_str += "b"


@xn(priority=1)
def c(a):
    sleep(T)
    pytest.compound_priority_str += "c"


@xn(priority=1)
def d(b):
    sleep(T)
    pytest.compound_priority_str += "d"


@xn(priority=1)
def e():
    sleep(T)
    pytest.compound_priority_str += "e"


@dag
def dependency_describer():
    _a = a()
    _b = b(a=_a)
    _c = c(a=_a)
    _d = d(b=_b)
    _e = e()


def test_compound_priority():
    dag = dependency_describer

    assert dag.node_dict_by_name["a"].compound_priority == 4
    assert dag.node_dict_by_name["b"].compound_priority == 2
    assert dag.node_dict_by_name["c"].compound_priority == 1
    assert dag.node_dict_by_name["d"].compound_priority == 1
    assert dag.node_dict_by_name["e"].compound_priority == 1


def test_compound_priority():
    pytest.compound_priority_str == ""
    dependency_describer()

    assert pytest.compound_priority_str.startswith("ab")
    assert len(pytest.compound_priority_str) == 5
