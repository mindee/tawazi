from typing import Dict, List, Tuple, TypeVar, Union

from tawazi import dag, xn

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


@xn
def stub(x: T) -> T:
    return x


def test_return_single_const() -> None:
    @dag
    def pipe() -> str:
        return "v1"

    assert pipe() == "v1"


def test_return_tuple_consts() -> None:
    @dag
    def pipe() -> Tuple[str, str, str]:
        return ("v1", "v2", "v3")

    assert pipe() == ("v1", "v2", "v3")


def test_return_tuple_consts_uxn() -> None:
    @dag
    def pipe() -> Tuple[str, str, str]:
        return (stub("v1"), "v2", stub("v3"))

    assert pipe() == ("v1", "v2", "v3")


def test_return_list_consts() -> None:
    @dag
    def pipe() -> List[str]:
        return ["v1", "v2", "v3"]

    assert pipe() == ["v1", "v2", "v3"]


def test_return_list_consts_uxn() -> None:
    @dag
    def pipe() -> List[str]:
        return [stub("v1"), "v2", stub("v3")]

    assert pipe() == ["v1", "v2", "v3"]


def test_return_dict_consts() -> None:
    @dag
    def pipe() -> Dict[str, str]:
        return {"r1": "v1", "r2": "v2", "r3": "r3"}

    assert pipe() == {"r1": "v1", "r2": "v2", "r3": "r3"}


def test_return_dict_consts_uxn() -> None:
    @dag
    def pipe() -> Dict[str, str]:
        return {"r1": "v1", "r2": stub("v2"), "r3": stub("r3")}

    assert pipe() == {"r1": "v1", "r2": "v2", "r3": "r3"}
