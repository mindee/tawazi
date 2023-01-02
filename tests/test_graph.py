#  type: ignore
from time import sleep

from networkx import NetworkXUnfeasible

from tawazi import DAG, ErrorStrategy
from tawazi.node import ExecNode

"""integration test"""

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
    ExecNode(a.__name__, a, [(None, c.__name__)], is_sequential=True),
    ExecNode(b.__name__, b, [(None, a.__name__)], priority=2, is_sequential=False),
    ExecNode(c.__name__, c, [(None, b.__name__)], priority=1, is_sequential=False),
]


def test_circular_deps():
    try:
        g = DAG(list_exec_nodes, 2, behavior=ErrorStrategy.strict)
    except NetworkXUnfeasible:
        pass
