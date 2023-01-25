# type: ignore
import pytest

from tawazi import op, to_dag
from tawazi.errors import TawaziTypeError


@op
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


def test_return_invalid_type():
    with pytest.raises(TawaziTypeError):

        @to_dag
        def pipe():
            return "bhasdfkjals"
