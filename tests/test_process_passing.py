from tawazi import dag, xn

global_var = 0


@xn
def add(x: int, y: int) -> int:
    global global_var
    global_var += 1
    return x + y


@dag
def pipe(i: int) -> int:
    a = add(i, 2)
    b = add(a, 3)
    c = add(a, 4)
    return add(b, c)


def test_pipe() -> None:
    global global_var
    assert pipe(1) == 13
    assert global_var == 4


def test_pipe_in_process_multiprocessing_on_dill() -> None:
    global global_var
    global_var = 0

    from multiprocessing_on_dill.pool import Pool

    with Pool() as ppe:
        results = ppe.map(pipe, range(10))
        assert results == [11, 13, 15, 17, 19, 21, 23, 25, 27, 29]
        assert global_var == 0


def test_pipe_in_process_pathos() -> None:
    global global_var
    global_var = 0

    from pathos.pools import ProcessPool

    with ProcessPool() as ppe:
        results = ppe.map(pipe, range(10))
        assert results == [11, 13, 15, 17, 19, 21, 23, 25, 27, 29]
        assert global_var == 0
