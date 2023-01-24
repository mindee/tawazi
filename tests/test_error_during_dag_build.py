# type: ignore
import pytest

from tawazi import node, op, to_dag


def test_execnodes():
    @op
    def a():
        pass

    with pytest.raises(NameError):

        @to_dag
        def pipe():
            a()
            b()  # an undefined ExecNode

    assert node.exec_nodes == []
