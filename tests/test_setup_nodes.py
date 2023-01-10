# type: ignore
from copy import deepcopy

import pytest

from tawazi import op, to_dag
from tawazi.errors import TawaziBaseException


def test_pipeline():
    @op(setup=True)
    def setup_op(in1):
        # setup operations should run a single time!
        #   even if invoked multiple times, they should run only once! and its result remain unchanged!
        pytest.setup_counter += 1
        return in1

    @op
    def op1(a_str: str):
        print("op1", a_str)
        pytest.op1_counter += 1
        return len(a_str)

    @to_dag
    def pipeline(in1, in2):
        a_str = setup_op(in1)
        return op1(a_str)

    @to_dag
    def pipeline2(in2):
        a_str = setup_op("mid_path")
        return op1(a_str)

    pytest.setup_counter = 0
    pytest.op1_counter = 0

    pipeline("s1", "s2")
    assert pytest.setup_counter == 1
    assert pytest.op1_counter == 1

    pipeline("s3", "s4")

    assert pytest.setup_counter == 1
    assert pytest.op1_counter == 2

    pipeline2("s5")

    assert pytest.setup_counter == 2
    assert pytest.op1_counter == 3


def test_bad_declaration():
    @op
    def op1():
        return True

    @op(setup=True)
    def setup_op(non_setup_result):
        return False

    with pytest.raises(TawaziBaseException):

        @to_dag
        def bad_pipe():
            setup_op(op1())


###############
# TEST ADVANCED
###############


def test_dependencies():
    @op(setup=True)
    def setup_op1():
        pytest.setup_op1 += 1
        return "sop1"

    @op(setup=True)
    def setup_op2(op1_result):
        pytest.setup_op2 += 1
        return "sop2"

    @op
    def op1(sop1_result):
        pytest.op1 += 1
        return "op1"

    @op
    def op2(sop2_result):
        pytest.op2 += 1
        return "op2"

    @op
    def op12(op1_result, op2_result):
        pytest.op12 += 1
        return "op12"

    @to_dag
    def pipe_setup_deps():
        sop1_r = setup_op1()
        sop2_r = setup_op2(sop1_r)
        op1_r = op1(sop1_r)
        op2_r = op2(sop2_r)
        op12_r = op12(op1_r, op2_r)

    pytest.setup_op1 = 0
    pytest.setup_op2 = 0
    pytest.op1 = 0
    pytest.op2 = 0
    pytest.op12 = 0

    pipe_setup_deps()
    assert pytest.setup_op1 == 1
    assert pytest.setup_op2 == 1
    assert pytest.op1 == 1
    assert pytest.op2 == 1
    assert pytest.op12 == 1

    pipe_setup_deps()
    assert pytest.setup_op1 == 1
    assert pytest.setup_op2 == 1
    assert pytest.op1 == 2
    assert pytest.op2 == 2
    assert pytest.op12 == 2


def test_dependencies_subgraph():
    @op(setup=True)
    def setup_op1():
        pytest.setup_op1 += 1
        return "sop1"

    @op(setup=True)
    def setup_op2(op1_result):
        pytest.setup_op2 += 1
        return "sop2"

    @op
    def op1(sop1_result):
        pytest.op1 += 1
        return "op1"

    @op
    def op2(sop2_result):
        pytest.op2 += 1
        return "op2"

    @op
    def op12(op1_result, op2_result):
        pytest.op12 += 1
        return "op12"

    @to_dag
    def pipe_setup_deps():
        sop1_r = setup_op1()
        sop2_r = setup_op2(sop1_r)
        op1_r = op1(sop1_r, __twz_tag="twinkle toes")
        op2_r = op2(sop2_r)
        op12_r = op12(op1_r, op2_r)
        return op12_r

    pytest.setup_op1 = 0
    pytest.setup_op2 = 0
    pytest.op1 = 0
    pytest.op2 = 0
    pytest.op12 = 0

    res = pipe_setup_deps(__twz_nodes=["twinkle toes"])
    assert res is None
    assert pytest.setup_op1 == 1
    assert pytest.setup_op2 == 0
    assert pytest.op1 == 1
    assert pytest.op2 == 0
    assert pytest.op12 == 0

    res = pipe_setup_deps(__twz_nodes=["twinkle toes"])
    assert res is None
    assert pytest.setup_op1 == 1
    assert pytest.setup_op2 == 0
    assert pytest.op1 == 2
    assert pytest.op2 == 0
    assert pytest.op12 == 0


test_dependencies_subgraph()
