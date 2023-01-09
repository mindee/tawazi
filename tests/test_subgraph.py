#  type: ignore
import pytest

from tawazi import _to_dag, op, to_dag
from tawazi.errors import TawaziBaseException

"""integration test"""

pytest.subgraph_comp_str = ""
T = 1e-3


@op
def a():
    pytest.subgraph_comp_str += "a"


@op
def b(a):
    pytest.subgraph_comp_str += "b"


@op
def c(a):
    pytest.subgraph_comp_str += "c"


@op
def d(c):
    pytest.subgraph_comp_str += "d"


@op
def e(c):
    pytest.subgraph_comp_str += "e"


@op
def f(e):
    pytest.subgraph_comp_str += "f"


@op
def g():
    pytest.subgraph_comp_str += "g"


@op
def h():
    pytest.subgraph_comp_str += "h"


@op
def i(h):
    pytest.subgraph_comp_str += "i"


@_to_dag
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
    dag = dag_describer()
    results = dag.execute([a, b, c, d, e, f, g, h, i])
    assert set("abcdefghi") == set(pytest.subgraph_comp_str)


def test_dag_subgraph_leaf_nodes():
    pytest.subgraph_comp_str = ""
    dag = dag_describer()
    results = dag.execute([b, d, f, g, i])
    assert set("abcdefghi") == set(pytest.subgraph_comp_str)


def test_dag_subgraph_leaf_nodes_with_extra_nodes():
    pytest.subgraph_comp_str = ""
    dag = dag_describer()
    results = dag.execute([b, c, e, h, g])
    assert set("abcegh") == set(pytest.subgraph_comp_str)


def test_dag_subgraph_nodes_ids():
    pytest.subgraph_comp_str = ""
    dag = dag_describer()
    results = dag.execute([b.id, c.id, e.id, h.id, g.id])
    assert set("abcegh") == set(pytest.subgraph_comp_str)


def test_dag_subgraph_non_existing_nodes_ids():
    with pytest.raises(ValueError, match="nodes are not in the graph"):
        dag = dag_describer()
        results = dag.execute(["gibirish"])


# TODO: fix this test
# def test_dag_subgraph_nodes_with_usage():
#     @to_dag
#     def pipe_duplication():
#         a()
#         a()
#     with pytest.raises(TawaziBaseException):
#         pipe_duplication.execute([a])
