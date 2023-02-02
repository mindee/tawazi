# type: ignore
import pytest

from tawazi import node, to_dag, xnode


def test_execnodes():
    @xnode
    def a():
        pass

    with pytest.raises(NameError):

        @to_dag
        def pipe():
            a()
            b()  # an undefined ExecNode

    assert node.exec_nodes == []
