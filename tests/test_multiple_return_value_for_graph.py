from typing import Dict, List, Tuple, TypeVar, Union

import pytest
from tawazi import dag, xn
from tawazi.errors import TawaziTypeError

T = TypeVar("T")


@xn
def a(v: T) -> T:
    return v


def test_no_return() -> None:
    @dag
    def pipe() -> None:
        return

    assert pipe() is None


def test_return_single() -> None:
    @dag
    def pipe() -> str:
        return a("twinkle")

    assert pipe() == "twinkle"


def test_return_tuple() -> None:
    @dag
    def pipe() -> Tuple[str, str, str]:
        res = a("tata")
        return res, res, res

    assert pipe() == ("tata", "tata", "tata")


def test_return_list() -> None:
    @dag
    def pipe() -> List[str]:
        res = a("tata")
        return [res, res, res]

    assert pipe() == ["tata", "tata", "tata"]


def test_return_dict() -> None:
    @dag
    def pipe() -> Dict[str, str]:
        res = a("tata")
        return {"1": res, "2": res, "3": res}

    assert pipe() == {"1": "tata", "2": "tata", "3": "tata"}


def test_return_dict2() -> None:
    @dag
    def pipe(a_const: int = 123) -> Dict[Union[str, int], Union[str, int]]:
        res = a("tata")
        return {1: res, "2": res, "input_value": a_const}

    assert pipe() == {1: "tata", "2": "tata", "input_value": 123}


def test_return_invalid_type() -> None:
    with pytest.raises(TawaziTypeError):

        @dag
        def pipe() -> str:
            return "bhasdfkjals"
