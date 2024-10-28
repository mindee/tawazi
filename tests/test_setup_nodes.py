from copy import deepcopy
from functools import reduce
from typing import Any

import pytest
from tawazi import dag, xn
from tawazi.errors import TawaziError, TawaziUsageError

from .common import run_pipeline


@pytest.mark.parametrize("is_async", [True, False])
def test_pipeline(is_async: bool) -> None:
    var_setup_counter = 0
    var_op1_counter = 0

    @xn(setup=True)
    def setup_op(in1: str) -> str:
        # setup operations should run a single time!
        #   even if invoked multiple times, they should run only once! and its result remain unchanged!
        nonlocal var_setup_counter, var_op1_counter
        var_setup_counter += 1
        return in1

    @xn
    def op1(a_str: str) -> int:
        nonlocal var_setup_counter, var_op1_counter
        var_op1_counter += 1
        return len(a_str)

    @dag(is_async=is_async)
    def pipeline(in1: str, in2: str) -> int:
        a_str = setup_op("in1")
        return op1(a_str)

    @dag(is_async=is_async)
    def pipeline2(in2: str) -> int:
        a_str = setup_op("mid_path")
        return op1(a_str)

    run_pipeline("s1", "s2", pipeline=pipeline, is_async=is_async)
    assert var_setup_counter == 1
    assert var_op1_counter == 1

    run_pipeline("s3", "s4", pipeline=pipeline, is_async=is_async)

    assert var_setup_counter == 1
    assert var_op1_counter == 2

    run_pipeline("s5", pipeline=pipeline2, is_async=is_async)

    assert var_setup_counter == 2
    assert var_op1_counter == 3


def test_bad_declaration() -> None:
    @xn
    def op1() -> bool:
        return True

    @xn(setup=True)
    def setup_op(non_setup_result: Any) -> bool:
        return False

    with pytest.raises(TawaziError):

        @dag
        def bad_pipe() -> None:
            setup_op(op1())


def test_not_setup_and_debug() -> None:
    with pytest.raises(ValueError):

        @xn(setup=True, debug=True)
        def op1() -> bool:
            return True

        @dag
        def bad_pipe() -> None:
            op1()


###############
# TEST ADVANCED
###############


def test_dependencies() -> None:
    var_setup_op1 = 0
    var_setup_op2 = 0
    var_op1 = 0
    var_op2 = 0
    var_op12 = 0

    @xn(setup=True)
    def setup_op1() -> str:
        nonlocal var_setup_op1, var_setup_op2, var_op1, var_op2, var_op12
        var_setup_op1 += 1
        return "sop1"

    @xn(setup=True)
    def setup_op2(op1_result: str) -> str:
        nonlocal var_setup_op1, var_setup_op2, var_op1, var_op2, var_op12
        var_setup_op2 += 1
        return "sop2"

    @xn
    def op1(sop1_result: str) -> str:
        nonlocal var_setup_op1, var_setup_op2, var_op1, var_op2, var_op12
        var_op1 += 1
        return "op1"

    @xn
    def op2(sop2_result: str) -> str:
        nonlocal var_setup_op1, var_setup_op2, var_op1, var_op2, var_op12
        var_op2 += 1
        return "op2"

    @xn
    def op12(op1_result: str, op2_result: str) -> str:
        nonlocal var_setup_op1, var_setup_op2, var_op1, var_op2, var_op12
        var_op12 += 1
        return "op12"

    @dag
    def pipe_setup_deps() -> None:
        sop1_r = setup_op1()
        sop2_r = setup_op2(sop1_r)
        op1_r = op1(sop1_r)
        op2_r = op2(sop2_r)
        op12(op1_r, op2_r)

    pipe_setup_deps()
    assert var_setup_op1 == 1
    assert var_setup_op2 == 1
    assert var_op1 == 1
    assert var_op2 == 1
    assert var_op12 == 1

    pipe_setup_deps()
    assert var_setup_op1 == 1
    assert var_setup_op2 == 1
    assert var_op1 == 2
    assert var_op2 == 2
    assert var_op12 == 2


def test_dependencies_subgraph() -> None:
    var_setup_op1 = 0
    var_setup_op2 = 0
    var_op1 = 0
    var_op2 = 0
    var_op12 = 0

    @xn(setup=True)
    def setup_op1() -> str:
        nonlocal var_setup_op1, var_setup_op2, var_op1, var_op2, var_op12
        var_setup_op1 += 1
        return "sop1"

    @xn(setup=True)
    def setup_op2(op1_result: str) -> str:
        nonlocal var_setup_op1, var_setup_op2, var_op1, var_op2, var_op12
        var_setup_op2 += 1
        return "sop2"

    @xn
    def op1(sop1_result: str) -> str:
        nonlocal var_setup_op1, var_setup_op2, var_op1, var_op2, var_op12
        var_op1 += 1
        return "op1"

    @xn
    def op2(sop2_result: str) -> str:
        nonlocal var_setup_op1, var_setup_op2, var_op1, var_op2, var_op12
        var_op2 += 1
        return "op2"

    @xn
    def op12(op1_result: str, op2_result: str) -> str:
        nonlocal var_setup_op1, var_setup_op2, var_op1, var_op2, var_op12
        var_op12 += 1
        return "op12"

    @dag
    def pipe_setup_deps() -> str:
        sop1_r = setup_op1()
        sop2_r = setup_op2(sop1_r)
        op1_r = op1(sop1_r, twz_tag="twinkle toes")  # type: ignore[call-arg]
        op2_r = op2(sop2_r)
        return op12(op1_r, op2_r)

    exec_ = pipe_setup_deps.executor(target_nodes=["twinkle toes"])
    res = exec_()
    assert res is None
    assert var_setup_op1 == 1
    assert var_setup_op2 == 0
    assert var_op1 == 1
    assert var_op2 == 0
    assert var_op12 == 0

    exec_ = pipe_setup_deps.executor(target_nodes=["twinkle toes"])
    res = exec_()
    assert res is None
    assert var_setup_op1 == 1
    assert var_setup_op2 == 0
    assert var_op1 == 2
    assert var_op2 == 0
    assert var_op12 == 0


@pytest.mark.parametrize("is_async", [True, False])
def test_pipeline_setup_method(is_async: bool) -> None:
    var_setup_op1 = 0
    var_setup_op2 = 0
    var_op1 = 0
    var_op2 = 0
    var_op12 = 0

    def clean() -> None:
        nonlocal var_setup_op1, var_setup_op2, var_op1, var_op2, var_op12
        var_setup_op1 = 0
        var_setup_op2 = 0
        var_op1 = 0
        var_op2 = 0
        var_op12 = 0

    @xn(setup=True)
    def setup_op1() -> str:
        nonlocal var_setup_op1, var_setup_op2, var_op1, var_op2, var_op12
        var_setup_op1 += 1
        return "sop1"

    @xn(setup=True)
    def setup_op2(op1_result: str) -> str:
        nonlocal var_setup_op1, var_setup_op2, var_op1, var_op2, var_op12
        var_setup_op2 += 1
        return "sop2"

    @xn
    def op1(sop1_result: str) -> str:
        nonlocal var_setup_op1, var_setup_op2, var_op1, var_op2, var_op12
        var_op1 += 1
        return "op1"

    @xn
    def op2(sop2_result: str) -> str:
        nonlocal var_setup_op1, var_setup_op2, var_op1, var_op2, var_op12
        var_op2 += 1
        return "op2"

    @xn
    def op12(op1_result: str, op2_result: str) -> str:
        nonlocal var_setup_op1, var_setup_op2, var_op1, var_op2, var_op12
        var_op12 += 1
        return "op12"

    @dag(is_async=is_async)
    def pipe_setup_deps() -> str:
        sop1_r = setup_op1(twz_tag="setup1")  # type: ignore[call-arg]
        sop2_r = setup_op2(sop1_r, twz_tag="setup2")  # type: ignore[call-arg]
        op1_r = op1(sop1_r, twz_tag="twinkle toes")  # type: ignore[call-arg]
        op2_r = op2(sop2_r)
        return op12(op1_r, op2_r)

    # test runninig setup without arguments
    pipe = deepcopy(pipe_setup_deps)
    clean()
    run_pipeline(pipeline=pipe.setup, is_async=is_async)
    assert var_setup_op1 == 1
    assert var_setup_op2 == 1
    assert var_op1 == 0
    assert var_op2 == 0
    assert var_op12 == 0
    run_pipeline(pipeline=pipe, is_async=is_async)

    assert var_setup_op1 == 1
    assert var_setup_op2 == 1
    assert var_op1 == 1
    assert var_op2 == 1
    assert var_op12 == 1

    # test running setup targeting a setup node
    pipe = deepcopy(pipe_setup_deps)
    clean()
    run_pipeline(pipeline=lambda: pipe.setup(target_nodes=["setup1"]), is_async=is_async)
    assert var_setup_op1 == 1
    assert var_setup_op2 == 0
    assert var_op1 == 0
    assert var_op2 == 0
    assert var_op12 == 0
    run_pipeline(pipeline=pipe, is_async=is_async)
    assert var_setup_op1 == 1
    assert var_setup_op2 == 1
    assert var_op1 == 1
    assert var_op2 == 1
    assert var_op12 == 1

    # test running setup targeting a dependencies of setup nodes
    pipe = deepcopy(pipe_setup_deps)
    clean()
    run_pipeline(pipeline=lambda: pipe.setup(target_nodes=["setup2"]), is_async=is_async)
    assert var_setup_op1 == 1
    assert var_setup_op2 == 1
    assert var_op1 == 0
    assert var_op2 == 0
    assert var_op12 == 0
    run_pipeline(pipeline=pipe, is_async=is_async)
    assert var_setup_op1 == 1
    assert var_setup_op2 == 1
    assert var_op1 == 1
    assert var_op2 == 1
    assert var_op12 == 1

    # test running setup targeting a non setup node
    pipe = deepcopy(pipe_setup_deps)
    clean()
    run_pipeline(pipeline=lambda: pipe.setup(target_nodes=["twinkle toes"]), is_async=is_async)
    assert var_setup_op1 == 1
    assert var_setup_op2 == 0
    assert var_op1 == 0
    assert var_op2 == 0
    assert var_op12 == 0
    exec_ = pipe.executor(target_nodes=["twinkle toes"])
    run_pipeline(pipeline=exec_, is_async=is_async)
    assert var_setup_op1 == 1
    assert var_setup_op2 == 0
    assert var_op1 == 1
    assert var_op2 == 0
    assert var_op12 == 0


def test_setup_node_cst_input() -> None:
    var_setop = 0

    @xn(setup=True)
    def setop(k: int = 1234) -> int:
        nonlocal var_setop
        var_setop += 1
        return k + 1

    @dag
    def pipe() -> int:
        return setop()

    @dag
    def pipe2() -> int:
        return setop(1)

    r = pipe()
    assert var_setop == 1
    assert r == 1235
    r = pipe()
    assert var_setop == 1
    assert r == 1235

    r2 = pipe2()
    assert var_setop == 2
    assert r2 == 2
    r2 = pipe2()
    assert var_setop == 2
    assert r2 == 2


def test_setup_no_default_arg() -> None:
    var_setop = 0

    @xn(setup=True)
    def setup(k: int) -> int:
        nonlocal var_setop
        var_setop += 1
        return k + 1

    @dag
    def pipe() -> int:
        return setup(10)

    r = pipe()
    assert r == 11
    assert var_setop == 1
    r = pipe()
    assert r == 11
    assert var_setop == 1


def test_setup_multiple_usages() -> None:
    var_get_model_setup = 0

    @xn(setup=True)
    def get_model(mid: str) -> str:
        nonlocal var_get_model_setup
        var_get_model_setup += 1
        return mid

    @xn
    def sumop(*mids: str) -> str:
        return reduce(str.__add__, mids, "")

    @dag
    def pipe() -> str:
        mid1 = get_model("a")
        mid2 = get_model("b")
        mid3 = get_model("c")
        mid4 = get_model("d")

        return sumop(mid1, mid2, mid3, mid4)

    pipe2 = deepcopy(pipe)

    r = pipe()
    assert r == "abcd"
    assert var_get_model_setup == 4
    r = pipe()
    assert r == "abcd"
    assert var_get_model_setup == 4

    var_get_model_setup = 0
    pipe2.setup()
    assert var_get_model_setup == 4
    r = pipe2()
    assert r == "abcd"
    assert var_get_model_setup == 4


def test_setup_xn_should_not_take_input_from_pipeline_args() -> None:
    @xn(setup=True)
    def setop(in1: Any) -> Any:
        pass

    with pytest.raises(TawaziUsageError):

        @dag
        def pipe(in1: Any) -> Any:
            setop(in1)
