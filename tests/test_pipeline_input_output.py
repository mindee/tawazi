# type: ignore

import pytest

from tawazi import op, to_dag
from tawazi.errors import TawaziArgumentException, TawaziBaseException


@op
def a(input_img, cst):
    return sum(input_img) + cst


@op
def lazy_print(*args):
    print(*args)


@to_dag
def declare_dag_function(input_img, cst: int = 0):
    lazy_print(cst)
    return a(input_img, cst)


@op
def op1(in1):
    return in1 + 1


def test_pipeline_input_output():
    l = declare_dag_function([1, 2, 3], 10)
    assert l == 16


def test_pipeline_input_output_skipping_default_params():
    l = declare_dag_function([1, 2, 3])
    assert l == 6


def test_pipeline_input_output_missing_argument():
    with pytest.raises(TawaziBaseException):
        l = declare_dag_function()


def test_pipeline_default_args_input_not_provided():
    @to_dag
    def pipe(in1=1, in2=2, in3=3, in4=4):
        return op1(in1), op1(in2), op1(in3), op1(in4)

    assert pipe() == (2, 3, 4, 5)


def test_pipeline_args_input_not_provided():
    # should fail!!
    @to_dag
    def pipe(in1, in2, in3, in4):
        return op1(in1), op1(in2), op1(in3), op1(in4)

    with pytest.raises(TawaziArgumentException):
        pipe()
