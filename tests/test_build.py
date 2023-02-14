# type: ignore
from time import sleep, time
from typing import Any

from tawazi import DAG, ErrorStrategy
from tawazi.node import ExecNode, XNWrapper

"""Internal Unit Test"""

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
    toto = 10 / 0
    return 1


# ExecNodes can be identified using the actual function or an identification string
en_a = XNWrapper(ExecNode(a.__name__, a, is_sequential=True))
en_b = XNWrapper(ExecNode(b.__name__, b, [en_a], priority=2, is_sequential=False))
en_c = XNWrapper(ExecNode(c.__name__, c, [en_a], priority=1, is_sequential=False))
en_d = XNWrapper(ExecNode(d.__name__, d, [en_b, en_c], priority=1, is_sequential=False))
en_e = XNWrapper(ExecNode(e.__name__, e, [en_b], is_sequential=False))
en_f = XNWrapper(ExecNode(f.__name__, f, [en_e], is_sequential=False))
en_g = XNWrapper(ExecNode(g.__name__, g, [en_e], is_sequential=False))

list_execnodes = [en_a, en_b, en_c, en_d, en_e, en_f, en_g]


failing_execnodes = list_execnodes + [ExecNode(fail.__name__, fail, [en_g], is_sequential=False)]


def test_dag_build() -> None:
    g: DAG[Any, Any] = DAG(list_execnodes, 2, behavior=ErrorStrategy.strict)
    t0 = time()
    g._execute(g._make_subgraph())  # must never fail!
    print(time() - t0)
    for k, v in g.node_dict.items():
        print(g, v, v.result)


def test_draw() -> None:
    g: DAG[Any, Any] = DAG(list_execnodes, 2, behavior=ErrorStrategy.strict)
    g.draw(display=False)
    g.draw(display=True)


def test_bad_behaviour() -> None:
    try:
        g: DAG[Any, Any] = DAG(failing_execnodes, 2, behavior="Such Bad Behavior")
        g._execute(g._make_subgraph())
    except NotImplementedError:
        pass
