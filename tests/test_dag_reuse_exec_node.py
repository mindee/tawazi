from tawazi import dag, xn


@xn
def a(v: int) -> int:
    return v + 1


@dag
def pipe() -> tuple[int, int]:
    a_1 = a(1)
    a_2 = a(2)

    return a_1, a_2


def test_function_reuse() -> None:
    a_1, a_2 = pipe()

    assert (a_1, a_2) == (2, 3)


@xn
def complex_function(a: int, b: int) -> int:
    return a + b


@dag
def pipe_reuse_with_positional_and_keyword_args() -> tuple[int, int, int]:
    a_1 = complex_function(1, 2)
    a_2 = complex_function(2, b=3)
    a_3 = complex_function(a=3, b=4)
    return a_1, a_2, a_3


def test_id_is_increasing() -> None:
    a_1, a_2, a_3 = pipe_reuse_with_positional_and_keyword_args()

    assert (a_1, a_2, a_3) == (3, 5, 7)
    assert pipe_reuse_with_positional_and_keyword_args.exec_nodes["complex_function"]
    assert pipe_reuse_with_positional_and_keyword_args.exec_nodes["complex_function<<1>>"]
    assert pipe_reuse_with_positional_and_keyword_args.exec_nodes["complex_function<<2>>"]
