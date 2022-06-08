from tawazi import ExecNode, DAG, ErrorStrategy
import logging
from time import time
from time import sleep

T = .1

def a():
    sleep(T)
    return "a"


def b(a):
    sleep(T)
    return a + "b"


def c(a):
    sleep(T)
    return a + "c"


def d(b, c):
    sleep(T)
    return b + c + "d"


def e(b):
    sleep(T)
    return b + "e"


def f(e):
    sleep(T)
    return e + "f"


def g(e):
    sleep(T)
    return e + "g"

# ExecNodes can be identified using the actual function or an identification string
l = [
    ExecNode(a, a, is_sequential=True),
    ExecNode("b", b, [a], priority=2, is_sequential=False),
    ExecNode(c, c, [a], priority=1, is_sequential=False),
    ExecNode(d, d, ["b", c], priority=1, is_sequential=False),
    ExecNode(e, e, ["b"], is_sequential=False),
    ExecNode(f, f, [e], is_sequential=False),
    ExecNode(g, g, [e], is_sequential=False),
]

def test_dag_build():
    g = DAG(l, 2, behaviour=ErrorStrategy.strict, logger=logging.getLogger())
    t0 = time()
    g.execute()  # must never fail!
    print(time() - t0)
    for k, v in g.node_dict.items():
        print(g, v, v.result)

