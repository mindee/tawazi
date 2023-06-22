from concurrent.futures import ProcessPoolExecutor

from tawazi import dag, xn

global_var = 0


@xn
def add(x: int, y: int) -> int:
    global global_var
    global_var += 1
    return x + y


@dag
def pipe() -> int:
    a = add(1, 2)
    b = add(a, 3)
    c = add(a, 4)
    return add(b, c)


def test_pipe() -> None:
    global global_var
    assert pipe() == 13
    assert global_var == 4


def test_pipe_in_process() -> None:
    global global_var
    global_var = 0
    with ProcessPoolExecutor() as ppe:
        assert ppe.submit(pipe).result() == 13
        assert global_var == 0


# test_pipe_in_process()
