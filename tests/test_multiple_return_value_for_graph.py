# type: ignore
import pytest

from tawazi import dag, xn
from tawazi.errors import TawaziTypeError


@xn
def a(v):
    return v


def test_no_return():
    @dag
    def pipe():
        return

    assert pipe() is None


def test_return_single():
    @dag
    def pipe():
        return a("twinkle")

    assert pipe() == "twinkle"


def test_return_tuple():
    @dag
    def pipe():
        res = a("tata")
        return res, res, res

    assert pipe() == ("tata", "tata", "tata")


def test_return_list():
    @dag
    def pipe():
        res = a("tata")
        return [res, res, res]

    assert pipe() == ["tata", "tata", "tata"]


def test_return_dict():
    @dag
    def pipe():
        res = a("tata")
        return {"1": res, "2": res, "3": res}

    assert pipe() == {"1": "tata", "2": "tata", "3": "tata"}


def test_return_dict2():
    @dag
    def pipe(a_const=123):
        res = a("tata")
        return {1: res, "2": res, "input_value": a_const}

    assert pipe() == {1: "tata", "2": "tata", "input_value": 123}


def test_return_invalid_type():
    with pytest.raises(TawaziTypeError):

        @dag
        def pipe():
            return "bhasdfkjals"
