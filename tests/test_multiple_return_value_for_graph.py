from typing import Union

from tawazi import dag

from .common import stub


def test_no_return() -> None:
    @dag
    def pipe() -> None:
        return

    assert pipe() is None


def test_return_single() -> None:
    @dag
    def pipe() -> str:
        return stub("twinkle")

    assert pipe() == "twinkle"


def test_return_tuple() -> None:
    @dag
    def pipe() -> tuple[str, str, str]:
        res = stub("tata")
        return res, res, res

    assert pipe() == ("tata", "tata", "tata")


def test_return_list() -> None:
    @dag
    def pipe() -> list[str]:
        res = stub("tata")
        return [res, res, res]

    assert pipe() == ["tata", "tata", "tata"]


def test_return_dict() -> None:
    @dag
    def pipe() -> dict[str, str]:
        res = stub("tata")
        return {"1": res, "2": res, "3": res}

    assert pipe() == {"1": "tata", "2": "tata", "3": "tata"}


def test_return_dict2() -> None:
    @dag
    def pipe(a_const: int = 123) -> dict[Union[str, int], Union[str, int]]:
        res = stub("tata")
        return {1: res, "2": res, "input_value": a_const}

    assert pipe() == {1: "tata", "2": "tata", "input_value": 123}


def test_return_single_const() -> None:
    @dag
    def pipe() -> str:
        return "v1"

    assert pipe() == "v1"


def test_return_tuple_consts() -> None:
    @dag
    def pipe() -> tuple[str, str, str]:
        return "v1", "v2", "v3"

    assert pipe() == ("v1", "v2", "v3")


def test_return_tuple_consts_uxn() -> None:
    @dag
    def pipe() -> tuple[str, str, str]:
        return stub("v1"), "v2", stub("v3")

    assert pipe() == ("v1", "v2", "v3")


def test_return_list_consts() -> None:
    @dag
    def pipe() -> list[str]:
        return ["v1", "v2", "v3"]

    assert pipe() == ["v1", "v2", "v3"]


def test_return_list_consts_uxn() -> None:
    @dag
    def pipe() -> list[str]:
        return [stub("v1"), "v2", stub("v3")]

    assert pipe() == ["v1", "v2", "v3"]


def test_return_dict_consts() -> None:
    @dag
    def pipe() -> dict[str, str]:
        return {"r1": "v1", "r2": "v2", "r3": "r3"}

    assert pipe() == {"r1": "v1", "r2": "v2", "r3": "r3"}


def test_return_dict_consts_uxn() -> None:
    @dag
    def pipe() -> dict[str, str]:
        return {"r1": "v1", "r2": stub("v2"), "r3": stub("r3")}

    assert pipe() == {"r1": "v1", "r2": "v2", "r3": "r3"}
