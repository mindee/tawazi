#  type: ignore
from time import sleep

from networkx import NetworkXUnfeasible

from tawazi import DAG, ErrorStrategy, ExecNode

T = 0.1


def a(c):
    sleep(T)
    return "a"


def b(a):
    sleep(T)
    return a + "b"


def c(b):
    sleep(T)
    return b + "c"


list_exec_nodes = [
    ExecNode(a, a, [c], is_sequential=True),
    ExecNode(b, b, [a], priority=2, is_sequential=False),
    ExecNode(c, c, [b], priority=1, is_sequential=False),
]


def test_circular_deps():
    try:
        g = DAG(list_exec_nodes, 2, behavior=ErrorStrategy.strict)
    except NetworkXUnfeasible:
        pass
