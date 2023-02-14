# type: ignore

from tawazi import dag, xn


@xn
def a(i):
    return i + 1


@xn
def b(i):
    return i + 1


@xn
def c(i):
    return i + 1


@xn
def d(i):
    return i + 1


@xn
def e(i):
    return i + 1


@dag
def pipe(i):
    a_ = a(i)
    b_ = b(a_)
    c_ = c(b_)
    d_ = d(c_)

    return d_


def test_basic_compose():

    composed_dag = pipe.compose([a], [b])
    assert pipe(0) == 4
    assert composed_dag(0) == 1
    assert pipe(0) == 4
    assert composed_dag(0) == 1


def test_full_pipe():
    composed_dag = pipe.compose(["pipe>>>i"], [d])
    assert pipe(0) == 4
    assert composed_dag(0) == 4


# def test_idk_what_to_name():
#     @dag
#     def pipe(i1, i2, i3, i4):
#         return (a(i1),b(i2),c(i3),d(i4))
