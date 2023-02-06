# type: ignore

import os

from tawazi import dag, xn


def test_non_pickalable_args():
    @xn
    def a(in1=1):
        if isinstance(in1, int):
            return in1 + 1
        return in1

    @xn
    def b(in1, in2):
        return in1 + in2

    @dag
    def pipe():

        return a(os), b(a(10), a())

    assert pipe() == (os, 13)
