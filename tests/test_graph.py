from time import sleep

import pytest
from networkx import NetworkXUnfeasible
from tawazi import DAG
from tawazi.node import ExecNode, UsageExecNode

T = 0.1


def a(c: str) -> str:
    sleep(T)
    return "a"


def b(a: str) -> str:
    sleep(T)
    return a + "b"


def c(b: str) -> str:
    sleep(T)
    return b + "c"


en_a = ExecNode(id_="a", exec_function=a, args=[], is_sequential=True)
en_b = ExecNode(
    id_="b", exec_function=b, args=[UsageExecNode(en_a.id)], priority=2, is_sequential=False
)
en_c = ExecNode(
    id_="c", exec_function=c, args=[UsageExecNode(en_a.id)], priority=1, is_sequential=False
)
en_a.args = [UsageExecNode(en_c.id)]


def test_circular_deps() -> None:
    with pytest.raises(NetworkXUnfeasible):
        DAG({xn.id: xn for xn in [en_a, en_b, en_c]}, [], [], 2)
