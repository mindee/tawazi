# type: ignore
import pytest

from tawazi import dag, xn
from tawazi.errors import InvalidExecNodeCall

"""integration test"""


# tests different cases of @op decoration for Python functions
# 1. different signatures
@xn
def f1():
    return 1


@xn
def f2(f1):
    return 2


@xn
def f3(f1, f2):
    assert f1 == 1
    assert f2 == 2
    return 3


@xn
def f4(f1, cst=0):
    # TODO: test with argument param and without argument param cst
    return 4 + cst


@xn
def f5(*args):
    return sum(args)


@xn
def f6(**kwargs):
    return sum(kwargs.values())


@xn
def f7(*args, **kwargs):
    return sum(args) + sum(kwargs.values())


@xn
def f8(f1, *args, **kwargs):
    return sum([f1, *args, *(kwargs.values())])


@dag
def pipe():
    _1 = f1()
    # import ipdb
    # ipdb.set_trace()
    _2 = f2(_1)
    _3 = f3(_1, _2)
    _4 = f4(_1)
    _5 = f5(_1, _2)
    _6 = f6(foo=_1, bar=_2)
    _7 = f7(_1, _2, foo=_1, bar=_2)
    _8 = f8(_1, _2, _3)


def test_ops_signatures():
    pipe()


def test_invalid_call_execnode():
    with pytest.raises(InvalidExecNodeCall):
        f6()
