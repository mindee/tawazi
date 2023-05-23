from time import sleep
from typing import Any

from tawazi import DAG, ErrorStrategy
from tawazi._node import ExecNode, UsageExecNode

T = 0.001
# global behavior_comp_str
behavior_comp_str = ""


def a() -> None:
    sleep(T)
    global behavior_comp_str
    behavior_comp_str += "a"


def b(a: Any) -> None:
    raise NotImplementedError


def c(b: Any) -> None:
    sleep(T)
    global behavior_comp_str
    behavior_comp_str += "c"


def d(a: Any) -> None:
    sleep(T)
    global behavior_comp_str
    behavior_comp_str += "d"


en_a = ExecNode(a.__qualname__, a, priority=1, is_sequential=False)
en_b = ExecNode(b.__qualname__, b, args=[UsageExecNode(en_a.id)], priority=2, is_sequential=False)
en_c = ExecNode(c.__qualname__, c, args=[UsageExecNode(en_b.id)], priority=2, is_sequential=False)
en_d = ExecNode(d.__qualname__, d, args=[UsageExecNode(en_a.id)], priority=1, is_sequential=False)
list_execnodes = [en_a, en_b, en_c, en_d]
node_dict = {xn.id: xn for xn in list_execnodes}


def test_strict_error_behavior() -> None:
    global behavior_comp_str
    behavior_comp_str = ""
    g: DAG[Any, Any] = DAG(node_dict, [], [], 1, behavior=ErrorStrategy.strict)
    try:
        g._execute(g._make_subgraph())
    except NotImplementedError:
        pass


def test_all_children_behavior() -> None:
    global behavior_comp_str
    behavior_comp_str = ""
    g: DAG[Any, Any] = DAG(node_dict, [], [], 1, behavior=ErrorStrategy.all_children)
    g._execute(g._make_subgraph())
    assert behavior_comp_str == "ad"


def test_permissive_behavior() -> None:
    global behavior_comp_str
    behavior_comp_str = ""
    g: DAG[Any, Any] = DAG(node_dict, [], [], 1, behavior=ErrorStrategy.permissive)
    g._execute(g._make_subgraph())
    assert behavior_comp_str == "acd"


# todo test using argname for ExecNode
