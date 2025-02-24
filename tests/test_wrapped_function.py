from tawazi import dag, xn


@xn
def abcd(i: int, b: list[str], cst: float = 0.1) -> int:
    """doc of a"""
    return i


@dag
def pipe(entry: int) -> int:
    """doc of my pipeline"""
    return abcd(entry, ["entry"])


def test_doc_pipeline() -> None:
    assert pipe.__doc__ == """doc of my pipeline"""


def test_name_pipeline() -> None:
    assert pipe.__name__ == "pipe"  # type: ignore[attr-defined]


def test_doc_operation() -> None:
    assert abcd.__doc__ == """doc of a"""


def test_name_op() -> None:
    assert abcd.__name__ == "abcd"  # type: ignore[attr-defined]
