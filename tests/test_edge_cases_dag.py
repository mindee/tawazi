import pytest
from tawazi import dag, xn


def test_same_constant_name_in_two_exec_nodes() -> None:
    @xn
    def a(cst: int) -> int:
        return cst

    @xn
    def b(a: int, cst: str) -> str:
        return str(a) + cst

    @dag
    def my_dag() -> None:
        var_a = a(1234)
        b(var_a, "poulpe")

    exec_nodes = my_dag._execute(my_dag._make_subgraph())
    assert len(exec_nodes) == 4
    assert exec_nodes[a.id].result == 1234
    assert exec_nodes[b.id].result == "1234poulpe"


def test_setup_debug_nodes() -> None:
    with pytest.raises(ValueError):

        @xn(debug=True, setup=True)
        def a():
            ...
