from typing import Dict, Tuple

from tawazi import dag, xn


def test_lazy_exec_nodes_return_dict_indexed() -> None:
    @xn
    def generate_dict() -> Dict[str, int]:
        return {"1": 1, "2": 2, "3": 3}

    @xn
    def incr(a: int) -> int:
        return a + 1

    @dag
    def pipe() -> Tuple[int, int, int]:
        d = generate_dict()
        return incr(d["1"]), incr(d["2"]), incr(d["3"])

    assert (2, 3, 4) == pipe()


def test_lazy_exec_nodes_multiple_return_values() -> None:
    @xn(unpack_to=3)
    def mulreturn() -> Tuple[int, int, int]:
        return 1, 2, 3

    @dag
    def pipe() -> Tuple[int, int]:
        r1, r2, _r3 = mulreturn()
        return r1, r2

    assert pipe() == (1, 2)
