from tawazi import dag, xn


def test_lazy_exec_nodes_return_dict_indexed():
    @xn
    def generate_dict():
        return {"1": 1, "2": 2, "3": 3}

    @xn
    def incr(a: int) -> int:
        return a + 1

    @dag
    def pipe():
        d = generate_dict()
        return incr(d["1"]), incr(d["2"]), incr(d["3"])

    assert (2, 3, 4) == pipe()
