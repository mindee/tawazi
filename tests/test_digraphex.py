import pytest
from tawazi._dag.digraph import DiGraphEx


@pytest.fixture
def graph() -> DiGraphEx:
    g = DiGraphEx()
    g.add_nodes_from([1, 2, 3, 4, 5, 6, 7])
    g.add_edges_from([(1, 2), (1, 3), (4, 5), (5, 6), (4, 7)])
    return g


def test_digraph_nodes(graph: DiGraphEx) -> None:
    assert graph.root_nodes == {1, 4}  # type: ignore[comparison-overlap]
    assert graph.leaf_nodes == [2, 3, 6, 7]  # type: ignore[comparison-overlap]


def test_digraph_succ_one_node(graph: DiGraphEx) -> None:
    succs = graph.single_node_successors(1)  # type: ignore[arg-type]
    assert succs == [1, 2, 3]  # type: ignore[comparison-overlap]


def test_digraph_succ_several_nodes(graph: DiGraphEx) -> None:
    succs = graph.multiple_nodes_successors([1, 5])  # type: ignore[list-item]
    assert succs == {1, 2, 3, 5, 6}  # type: ignore[comparison-overlap]


def test_graph_remove_recursively(graph: DiGraphEx) -> None:
    h = DiGraphEx(graph)
    h.remove_recursively(1)  # type: ignore[arg-type]
    assert set(h.nodes) == {4, 5, 6, 7}


def test_graph_remove_recursively_without_root(graph: DiGraphEx) -> None:
    h = DiGraphEx(graph)
    h.remove_recursively(1, remove_root_node=False)  # type: ignore[arg-type]
    assert set(h.nodes) == {1, 4, 5, 6, 7}


def test_minimal_induced_subgraph(graph: DiGraphEx) -> None:
    h = DiGraphEx(graph)
    h = h.minimal_induced_subgraph([4, 5, 6, 7]).copy()  # type: ignore[list-item]
    assert set(h.nodes) == {4, 5, 6, 7}
    assert list(h.edges) == [(4, 5), (4, 7), (5, 6)]

    with pytest.raises(ValueError):
        h.minimal_induced_subgraph(["zaza", "zozo"])


def test_topological_sort(graph: DiGraphEx) -> None:
    ts = graph.topologically_sorted
    assert ts == [1, 4, 2, 3, 5, 7, 6]  # type: ignore[comparison-overlap]


def test_ancestors_of_iter(graph: DiGraphEx) -> None:
    anc = graph.ancestors_of_iter([6])  # type: ignore[list-item]
    assert anc == {4, 5}  # type: ignore[comparison-overlap]
