# type: ignore

from tawazi import op, to_dag


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

    l = declare_dag_function([1, 2, 3])
    assert l == 6
