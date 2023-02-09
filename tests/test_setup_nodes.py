# type: ignore
from copy import deepcopy
from functools import reduce

import pytest

from tawazi import to_dag, xn
from tawazi.errors import TawaziBaseException, TawaziUsageError


def test_pipeline():
    @xn(setup=True)
    def setup_op(in1):
        # setup operations should run a single time!
        #   even if invoked multiple times, they should run only once! and its result remain unchanged!
        pytest.setup_counter += 1
        return in1

    @xn
    def op1(a_str: str):
        print("op1", a_str)
        pytest.op1_counter += 1
        return len(a_str)

    @to_dag
    def pipeline(in1, in2):
        a_str = setup_op("in1")
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
    @xn
    def op1():
        return True

    @xn(setup=True)
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
    @xn(setup=True)
    def setup_op1():
        pytest.setup_op1 += 1
        return "sop1"

    @xn(setup=True)
    def setup_op2(op1_result):
        pytest.setup_op2 += 1
        return "sop2"

    @xn
    def op1(sop1_result):
        pytest.op1 += 1
        return "op1"

    @xn
    def op2(sop2_result):
        pytest.op2 += 1
        return "op2"

    @xn
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
    @xn(setup=True)
    def setup_op1():
        pytest.setup_op1 += 1
        return "sop1"

    @xn(setup=True)
    def setup_op2(op1_result):
        pytest.setup_op2 += 1
        return "sop2"

    @xn
    def op1(sop1_result):
        pytest.op1 += 1
        return "op1"

    @xn
    def op2(sop2_result):
        pytest.op2 += 1
        return "op2"

    @xn
    def op12(op1_result, op2_result):
        pytest.op12 += 1
        return "op12"

    @to_dag
    def pipe_setup_deps():
        sop1_r = setup_op1()
        sop2_r = setup_op2(sop1_r)
        op1_r = op1(sop1_r, twz_tag="twinkle toes")
        op2_r = op2(sop2_r)
        op12_r = op12(op1_r, op2_r)
        return op12_r

    pytest.setup_op1 = 0
    pytest.setup_op2 = 0
    pytest.op1 = 0
    pytest.op2 = 0
    pytest.op12 = 0

    res = pipe_setup_deps(twz_nodes=["twinkle toes"])
    assert res is None
    assert pytest.setup_op1 == 1
    assert pytest.setup_op2 == 0
    assert pytest.op1 == 1
    assert pytest.op2 == 0
    assert pytest.op12 == 0

    res = pipe_setup_deps(twz_nodes=["twinkle toes"])
    assert res is None
    assert pytest.setup_op1 == 1
    assert pytest.setup_op2 == 0
    assert pytest.op1 == 2
    assert pytest.op2 == 0
    assert pytest.op12 == 0


def test_pipeline_setup_method():
    def clean():
        pytest.setup_op1 = 0
        pytest.setup_op2 = 0
        pytest.op1 = 0
        pytest.op2 = 0
        pytest.op12 = 0

    @xn(setup=True)
    def setup_op1():
        pytest.setup_op1 += 1
        return "sop1"

    @xn(setup=True)
    def setup_op2(op1_result):
        pytest.setup_op2 += 1
        return "sop2"

    @xn
    def op1(sop1_result):
        pytest.op1 += 1
        return "op1"

    @xn
    def op2(sop2_result):
        pytest.op2 += 1
        return "op2"

    @xn
    def op12(op1_result, op2_result):
        pytest.op12 += 1
        return "op12"

    @to_dag
    def pipe_setup_deps():
        sop1_r = setup_op1(twz_tag="setup1")
        sop2_r = setup_op2(sop1_r, twz_tag="setup2")
        op1_r = op1(sop1_r, twz_tag="twinkle toes")
        op2_r = op2(sop2_r)
        op12_r = op12(op1_r, op2_r)
        return op12_r

    # test runninig setup without arguments
    pipe = deepcopy(pipe_setup_deps)
    clean()
    pipe.setup()
    assert pytest.setup_op1 == 1
    assert pytest.setup_op2 == 1
    assert pytest.op1 == 0
    assert pytest.op2 == 0
    assert pytest.op12 == 0
    pipe()

    assert pytest.setup_op1 == 1
    assert pytest.setup_op2 == 1
    assert pytest.op1 == 1
    assert pytest.op2 == 1
    assert pytest.op12 == 1

    # test running setup targetting a setup node
    pipe = deepcopy(pipe_setup_deps)
    clean()
    pipe.setup(twz_nodes=["setup1"])
    assert pytest.setup_op1 == 1
    assert pytest.setup_op2 == 0
    assert pytest.op1 == 0
    assert pytest.op2 == 0
    assert pytest.op12 == 0
    pipe()
    assert pytest.setup_op1 == 1
    assert pytest.setup_op2 == 1
    assert pytest.op1 == 1
    assert pytest.op2 == 1
    assert pytest.op12 == 1

    # test running setup targeting a dependencies of setup nodes
    pipe = deepcopy(pipe_setup_deps)
    clean()
    pipe.setup(twz_nodes=["setup2"])
    assert pytest.setup_op1 == 1
    assert pytest.setup_op2 == 1
    assert pytest.op1 == 0
    assert pytest.op2 == 0
    assert pytest.op12 == 0
    pipe()
    assert pytest.setup_op1 == 1
    assert pytest.setup_op2 == 1
    assert pytest.op1 == 1
    assert pytest.op2 == 1
    assert pytest.op12 == 1

    # test running setup targetting a non setup node
    pipe = deepcopy(pipe_setup_deps)
    clean()
    pipe.setup(twz_nodes=["twinkle toes"])
    assert pytest.setup_op1 == 1
    assert pytest.setup_op2 == 0
    assert pytest.op1 == 0
    assert pytest.op2 == 0
    assert pytest.op12 == 0
    pipe(twz_nodes=["twinkle toes"])
    assert pytest.setup_op1 == 1
    assert pytest.setup_op2 == 0
    assert pytest.op1 == 1
    assert pytest.op2 == 0
    assert pytest.op12 == 0


def test_setup_node_cst_input():
    @xn(setup=True)
    def setop(k: int = 1234):
        pytest.setop += 1
        return k + 1

    @to_dag
    def pipe():
        return setop()

    @to_dag
    def pipe2():
        return setop(1)

    pytest.setop = 0
    r = pipe()
    assert pytest.setop == 1
    assert r == 1235
    r = pipe()
    assert pytest.setop == 1
    assert r == 1235

    r2 = pipe2()
    assert pytest.setop == 2
    assert r2 == 2
    r2 = pipe2()
    assert pytest.setop == 2
    assert r2 == 2


def test_setup_no_default_arg():
    @xn(setup=True)
    def setup(k: int):
        pytest.setop += 1
        return k + 1

    @to_dag
    def pipe():
        return setup(10)

    pytest.setop = 0
    r = pipe()
    assert r == 11
    assert pytest.setop == 1
    r = pipe()
    assert r == 11
    assert pytest.setop == 1


def test_setup_multiple_usages():
    @xn(setup=True)
    def get_model(mid: str):
        pytest.get_model_setup += 1
        return mid

    @xn
    def sumop(*mids):
        return reduce(str.__add__, mids)

    @to_dag
    def pipe():
        mid1 = get_model("a")
        mid2 = get_model("b")
        mid3 = get_model("c")
        mid4 = get_model("d")

        return sumop(mid1, mid2, mid3, mid4)

    pipe2 = deepcopy(pipe)

    pytest.get_model_setup = 0
    r = pipe()
    assert r == "abcd"
    assert pytest.get_model_setup == 4
    r = pipe()
    assert r == "abcd"
    assert pytest.get_model_setup == 4

    pytest.get_model_setup = 0
    pipe2.setup()
    assert pytest.get_model_setup == 4
    r = pipe2()
    assert r == "abcd"
    assert pytest.get_model_setup == 4


def test_setup_xn_should_not_take_input_from_pipeline_args():
    @xn(setup=True)
    def setop(in1):
        pass

    with pytest.raises(TawaziUsageError):

        @to_dag
        def pipe(in1):
            setop(in1)
