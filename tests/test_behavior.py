#  type: ignore
from time import sleep

import pytest

from tawazi import DAG, ErrorStrategy
from tawazi.node import ExecNode

"""Internal Unit tests"""


T = 0.001
# global behavior_comp_str
pytest.behavior_comp_str = ""


def a():
    sleep(T)
    pytest.behavior_comp_str += "a"


def b(a):
    raise NotImplementedError


def c(b):
    sleep(T)
    pytest.behavior_comp_str += "c"


def d(a):
    sleep(T)
    pytest.behavior_comp_str += "d"


en_a = ExecNode(a.__name__, a, priority=1, is_sequential=False)
en_b = ExecNode(b.__name__, b, args=[en_a], priority=2, is_sequential=False)
en_c = ExecNode(c.__name__, c, args=[en_b], priority=2, is_sequential=False)
en_d = ExecNode(d.__name__, d, args=[en_a], priority=1, is_sequential=False)
list_execnodes = [en_a, en_b, en_c, en_d]


def test_strict_error_behavior():
    pytest.behavior_comp_str = ""
    g = DAG(list_execnodes, 1, behavior=ErrorStrategy.strict)
    try:
        g.execute()
    except NotImplementedError:
        pass


def test_all_children_behavior():
    pytest.behavior_comp_str = ""
    g = DAG(list_execnodes, 1, behavior=ErrorStrategy.all_children)
    g.execute()
    assert pytest.behavior_comp_str == "ad"


def test_permissive_behavior():
    pytest.behavior_comp_str = ""
    g = DAG(list_execnodes, 1, behavior=ErrorStrategy.permissive)
    g.execute()
    assert pytest.behavior_comp_str == "acd"


# todo test using argname for ExecNode
