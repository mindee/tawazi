import pytest
from tawazi import dag, node, xn


def test_execnodes() -> None:
    @xn
    def a() -> None:
        pass

    with pytest.raises(NameError):

        @dag
        def pipe() -> None:
            a()
            # purposefully an undefined ExecNode
            b()  # type: ignore[name-defined] # noqa: F821

    assert node.node.exec_nodes == {}
