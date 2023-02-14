# type: ignore
from time import sleep
from typing import Any

import pytest

from tawazi import DAG, ErrorStrategy
from tawazi.node import ExecNode, XNWrapper

"""Internal Unit tests"""


T = 0.001
# global behavior_comp_str
pytest.behavior_comp_str: str = ""


def a() -> None:
    sleep(T)
    pytest.behavior_comp_str += "a"


def b(a: Any) -> None:
    raise NotImplementedError


def c(b: Any) -> None:
    sleep(T)
    pytest.behavior_comp_str += "c"


def d(a: Any) -> None:
    sleep(T)
    pytest.behavior_comp_str += "d"


en_a = XNWrapper(ExecNode(a.__qualname__, a, priority=1, is_sequential=False))
en_b = XNWrapper(ExecNode(b.__qualname__, b, args=[en_a], priority=2, is_sequential=False))
en_c = XNWrapper(ExecNode(c.__qualname__, c, args=[en_b], priority=2, is_sequential=False))
en_d = XNWrapper(ExecNode(d.__qualname__, d, args=[en_a], priority=1, is_sequential=False))
list_execnodes = [en_a, en_b, en_c, en_d]


def test_strict_error_behavior() -> None:
    pytest.behavior_comp_str = ""
    g: DAG[Any, Any] = DAG(list_execnodes, 1, behavior=ErrorStrategy.strict)
    try:
        g._execute(g._make_subgraph())
    except NotImplementedError:
        pass


def test_all_children_behavior() -> None:
    pytest.behavior_comp_str = ""
    g: DAG[Any, Any] = DAG(list_execnodes, 1, behavior=ErrorStrategy.all_children)
    g._execute(g._make_subgraph())
    assert pytest.behavior_comp_str == "ad"


def test_permissive_behavior() -> None:
    pytest.behavior_comp_str = ""
    g: DAG[Any, Any] = DAG(list_execnodes, 1, behavior=ErrorStrategy.permissive)
    g._execute(g._make_subgraph())
    assert pytest.behavior_comp_str == "acd"


# todo test using argname for ExecNode
