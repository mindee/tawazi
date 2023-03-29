from tawazi import dag, xn


@xn(tag="b")
def a(b: int = 1234) -> int:
    return b + 1


@xn(tag=("op", "b", "takes argument a"))
def b(a: int = 4321) -> int:
    return a + 1


@dag
def pipe() -> int:
    return b(a())


def test_tag() -> None:
    assert pipe() == 1236
    assert pipe.get_nodes_by_tag("b") == [pipe.get_node_by_id("a"), pipe.get_node_by_id("b")]
    assert pipe.get_nodes_by_tag("op") == [pipe.get_node_by_id("b")]
    assert pipe.get_nodes_by_tag("takes argument a") == [pipe.get_node_by_id("b")]


def test_call_tag() -> None:
    @dag
    def pipe() -> int:
        a_ = a(10, twz_tag="another_a_tag")  # type: ignore[call-arg]
        return b(a_, twz_tag="another_b_tag")  # type: ignore[call-arg]

    assert pipe() == 12
    assert pipe.get_nodes_by_tag("another_a_tag") == [pipe.get_node_by_id("a")]
    assert pipe.get_nodes_by_tag("another_b_tag") == [pipe.get_node_by_id("b")]
