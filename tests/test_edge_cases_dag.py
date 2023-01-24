#  type: ignore
from tawazi import op, to_dag

"""integration test"""


def test_same_constant_name_in_two_exec_nodes():
    @op
    def a(cst: int):
        print(cst)
        return cst

    @op
    def b(a, cst: str):
        print(a, cst)
        return str(a) + cst

    @to_dag
    def my_dag():
        var_a = a(1234)
        var_b = b(var_a, "poulpe")

    exec_nodes = my_dag.execute()
    assert len(exec_nodes) == 4
    assert exec_nodes[a.id].result == 1234
    assert exec_nodes[b.id].result == "1234poulpe"
