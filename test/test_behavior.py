from tawazi import ExecNode, DAG, ErrorStrategy

import logging
from time import sleep
import pytest

T = .001
# global comp_str
pytest.comp_str = ""

def a():
    sleep(T)
    pytest.comp_str += "a"


def b(a):
    raise NotImplementedError
    sleep(T)
    pytest.comp_str += "b"


def c(b):
    sleep(T)
    pytest.comp_str += "c"


def d(a):
    sleep(T)
    pytest.comp_str += "d"

l = [
    ExecNode(a, a, priority=1, is_sequential=False),
    ExecNode(b, b, [a], priority=2, is_sequential=False),
    ExecNode(c, c, [b], priority=2, is_sequential=False),
    ExecNode(d, d, [a], priority=1, is_sequential=False),
]

def test_strict_error_behavior():
    pytest.comp_str = ""
    g = DAG(l, 1, behaviour=ErrorStrategy.strict, logger=logging.getLogger())
    try:
        g.execute()
    except NotImplementedError:
        pass

def test_all_children_behavior():
    pytest.comp_str = ""
    g = DAG(l, 1, behaviour=ErrorStrategy.all_children, logger=logging.getLogger())
    g.execute()
    assert pytest.comp_str == "ad"

def test_permissive_behavior():
    pytest.comp_str = ""
    g = DAG(l, 1, behaviour=ErrorStrategy.permissive, logger=logging.getLogger())
    g.execute()
    assert pytest.comp_str == "acd"

# todo test using argname for ExecNode
