# type: ignore
import pytest

from tawazi import to_dag, xnode
from tawazi.errors import TawaziTypeError


@xnode
def a(v):
    return v


def test_no_return():
    @to_dag
    def pipe():
        return

    assert pipe() == None


def test_return_single():
    @to_dag
    def pipe():
        return a("twinkle")

    assert pipe() == "twinkle"


def test_return_tuple():
    @to_dag
    def pipe():
        res = a("tata")
        return res, res, res

    assert pipe() == ("tata", "tata", "tata")


def test_return_list():
    @to_dag
    def pipe():
        res = a("tata")
        return [res, res, res]

    assert pipe() == ["tata", "tata", "tata"]


def test_return_dict():
    @to_dag
    def pipe():
        res = a("tata")
        return {"1": res, "2": res, "3": res}

    assert pipe() == {"1": "tata", "2": "tata", "3": "tata"}


def test_return_dict2():
    @to_dag
    def pipe(a_const=123):
        res = a("tata")
        return {1: res, "2": res, "input_value": a_const}

    assert pipe() == {1: "tata", "2": "tata", "input_value": 123}


def test_return_invalid_type():
    with pytest.raises(TawaziTypeError):

        @to_dag
        def pipe():
            return "bhasdfkjals"
