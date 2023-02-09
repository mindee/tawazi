# type: ignore
from tawazi import to_dag, xn


@xn
def a(v):
    return v + 1


@to_dag
def pipe():
    a_1 = a(1)
    a_2 = a(2)

    return a_1, a_2


def test_function_reuse():
    a_1, a_2 = pipe()

    assert a_1, a_2 == (2, 3)
