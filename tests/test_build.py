from time import sleep
from typing import Any, cast

import pytest
from tawazi import DAG, ErrorStrategy
from tawazi.node import ExecNode, UsageExecNode

T = 0.1


def a() -> str:
    sleep(T)
    return "a"


def b(a: str) -> str:
    sleep(T)
    return a + "b"


def c(a: str) -> str:
    sleep(T)
    return a + "c"


def d(b: str, c: str) -> str:
    sleep(T)
    return b + c + "d"


def e(b: str) -> str:
    sleep(T)
    return b + "e"


def f(e: str) -> str:
    sleep(T)
    return e + "f"


def g(e: str) -> str:
    sleep(T)
    return e + "g"


def fail(g: Any) -> int:
    return cast(int, 10 / 0)


# ExecNodes can be identified using the actual function or an identification string
en_a = ExecNode(a.__name__, a, is_sequential=True)
en_b = ExecNode(b.__name__, b, [UsageExecNode(en_a.id)], priority=2, is_sequential=False)
en_c = ExecNode(c.__name__, c, [UsageExecNode(en_a.id)], priority=1, is_sequential=False)
en_d = ExecNode(
    d.__name__, d, [UsageExecNode(en_b.id), UsageExecNode(en_c.id)], priority=1, is_sequential=False
)
en_e = ExecNode(e.__name__, e, [UsageExecNode(en_b.id)], is_sequential=False)
en_f = ExecNode(f.__name__, f, [UsageExecNode(en_e.id)], is_sequential=False)
en_g = ExecNode(g.__name__, g, [UsageExecNode(en_e.id)], is_sequential=False)

list_execnodes = [en_a, en_b, en_c, en_d, en_e, en_f, en_g]
node_dict = {xn.id: xn for xn in list_execnodes}

failing_execnodes = list_execnodes + [
    ExecNode(fail.__name__, fail, [UsageExecNode(en_g.id)], is_sequential=False)
]
failing_node_dict = {xn.id: xn for xn in failing_execnodes}


def test_dag_build() -> None:
    g: DAG[Any, Any] = DAG(node_dict, [], [], 2, behavior=ErrorStrategy.strict)
    g._execute(g._make_subgraph())  # must never fail!


def test_draw() -> None:
    g: DAG[Any, Any] = DAG(node_dict, [], [], 2, behavior=ErrorStrategy.strict)
    g.draw(display=False)
    g.draw(display=True)


def test_bad_behaviour() -> None:
    try:
        g: DAG[Any, Any] = DAG(failing_node_dict, [], [], 2, behavior="Such Bad Behavior")  # type: ignore[arg-type]
        g._execute(g._make_subgraph())
    except NotImplementedError:
        pass


def test_setting_execnode_id_should_fail() -> None:
    with pytest.raises(AttributeError):
        en_a.id = "fdsakfjs"  # type: ignore[misc]
