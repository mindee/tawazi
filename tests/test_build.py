#  type: ignore
from time import sleep, time

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
en_a = ExecNode(a.__name__, a, is_sequential=True)
en_b = ExecNode(b.__name__, b, [en_a], priority=2, is_sequential=False)
en_c = ExecNode(c.__name__, c, [en_a], priority=1, is_sequential=False)
en_d = ExecNode(d.__name__, d, [en_b, en_c], priority=1, is_sequential=False)
en_e = ExecNode(e.__name__, e, [en_b], is_sequential=False)
en_f = ExecNode(f.__name__, f, [en_e], is_sequential=False)
en_g = ExecNode(g.__name__, g, [en_e], is_sequential=False)

list_execnodes = [en_a, en_b, en_c, en_d, en_e, en_f, en_g]


failing_execnodes = list_execnodes + [ExecNode(fail.__name__, fail, [en_g], is_sequential=False)]


def test_dag_build():
    g = DAG(list_execnodes, 2, behavior=ErrorStrategy.strict)
    t0 = time()
    g.execute()  # must never fail!
    print(time() - t0)
    for k, v in g.node_dict.items():
        print(g, v, v.result)


def test_draw():
    g = DAG(list_execnodes, 2, behavior=ErrorStrategy.strict)
    g.draw(display=False)
    g.draw(display=True)


def test_bad_behaviour():
    try:
        g = DAG(failing_execnodes, 2, behavior="Such Bad Behavior")
        g.execute()
    except NotImplementedError:
        pass
