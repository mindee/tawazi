#  type: ignore
from time import sleep, time

import pydantic
import pytest

from tawazi import DAG, ErrorStrategy
from tawazi.node import ExecNode

"""Internal Unit Test"""

T = 0.1


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


def fail(g):
    toto = 10 / 0
    return 1


# ExecNodes can be identified using the actual function or an identification string
en_a = ExecNode(id=a.__name__, exec_function=a, is_sequential=True)
en_b = ExecNode(id=b.__name__, exec_function=b, args=[en_a], priority=2, is_sequential=False)
en_c = ExecNode(id=c.__name__, exec_function=c, args=[en_a], priority=1, is_sequential=False)
en_d = ExecNode(id=d.__name__, exec_function=d, args=[en_b, en_c], priority=1, is_sequential=False)
en_e = ExecNode(id=e.__name__, exec_function=e, args=[en_b], is_sequential=False)
en_f = ExecNode(id=f.__name__, exec_function=f, args=[en_e], is_sequential=False)
en_g = ExecNode(id=g.__name__, exec_function=g, args=[en_e], is_sequential=False)

list_execnodes = [en_a, en_b, en_c, en_d, en_e, en_f, en_g]


failing_execnodes = list_execnodes + [
    ExecNode(id=fail.__name__, exec_function=fail, args=[en_g], is_sequential=False)
]


def test_dag_build():
    g = DAG(exec_nodes=list_execnodes, max_concurrency=2, behavior=ErrorStrategy.strict)
    t0 = time()
    g._execute(g._make_subgraph())  # must never fail!
    print(time() - t0)
    for k, v in g.node_dict.items():
        print(g, v, v.result)


def test_draw():
    g = DAG(exec_nodes=list_execnodes, max_concurrency=2, behavior=ErrorStrategy.strict)
    g.draw(display=False)
    g.draw(display=True)


def test_bad_behaviour():
    with pytest.raises(pydantic.ValidationError):
        g = DAG(exec_nodes=failing_execnodes, max_concurrency=2, behavior="Such Bad Behavior")
        g._execute(g._make_subgraph())
