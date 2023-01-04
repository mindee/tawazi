# type: ignore

import pytest

from tawazi import op, to_dag
from tawazi.errors import TawaziBaseException


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


def test_pipeline_input_output():
    l = declare_dag_function([1, 2, 3], 10)
    assert l == 16


def test_pipeline_input_output_skipping_default_params():
    l = declare_dag_function([1, 2, 3])
    assert l == 6


def test_pipeline_input_output_missing_argument():
    with pytest.raises(TawaziBaseException):
        l = declare_dag_function()
