#  type: ignore
import pytest

from tawazi import to_dag, xnode
from tawazi.errors import TawaziBaseException

"""integration test"""

pytest.subgraph_comp_str = ""
T = 1e-3


@xnode
def a():
    pytest.subgraph_comp_str += "a"


@xnode
def b(a):
    pytest.subgraph_comp_str += "b"


@xnode
def c(a):
    pytest.subgraph_comp_str += "c"


@xnode
def d(c):
    pytest.subgraph_comp_str += "d"


@xnode
def e(c):
    pytest.subgraph_comp_str += "e"


@xnode
def f(e):
    pytest.subgraph_comp_str += "f"


@xnode
def g():
    pytest.subgraph_comp_str += "g"


@xnode
def h():
    pytest.subgraph_comp_str += "h"


@xnode
def i(h):
    pytest.subgraph_comp_str += "i"


@to_dag
def dag_describer():
    var_a = a()
    var_b = b(var_a)
    var_c = c(var_a)
    var_d = d(var_c)
    var_e = e(var_c)
    var_f = f(var_e)

    var_g = g()

    var_h = h()
    var_i = i(var_h)


def test_dag_subgraph_all_nodes():
    pytest.subgraph_comp_str = ""
    dag = dag_describer
    results = dag._execute([a, b, c, d, e, f, g, h, i])
    assert set("abcdefghi") == set(pytest.subgraph_comp_str)


def test_dag_subgraph_leaf_nodes():
    pytest.subgraph_comp_str = ""
    dag = dag_describer
    results = dag._execute([b, d, f, g, i])
    assert set("abcdefghi") == set(pytest.subgraph_comp_str)


def test_dag_subgraph_leaf_nodes_with_extra_nodes():
    pytest.subgraph_comp_str = ""
    dag = dag_describer
    results = dag._execute([b, c, e, h, g])
    assert set("abcegh") == set(pytest.subgraph_comp_str)


def test_dag_subgraph_nodes_ids():
    pytest.subgraph_comp_str = ""
    dag = dag_describer
    results = dag._execute([b.id, c.id, e.id, h.id, g.id])
    assert set("abcegh") == set(pytest.subgraph_comp_str)


def test_dag_subgraph_non_existing_nodes_ids():
    with pytest.raises(ValueError, match="nodes are not in the graph"):
        dag = dag_describer
        results = dag._execute(["gibirish"])


@xnode
def a1(in1):
    return in1 + 1


@to_dag
def pipe(in1):
    return a1(in1)


def test_no_nodes_running_in_subgraph():
    assert pipe(twz_nodes=[]) == None


# TODO: fix this problem!!!
# def test_dag_subgraph_nodes_with_usage():
#     @to_dag
#     def pipe_duplication():
#         a()
#         a()
#     with pytest.raises(TawaziBaseException):
#         pipe_duplication.execute([a])
