# type: ignore
from typing import List

from tawazi import dag, xn


@xn
def abcd(i: int, b: List[str], cst: float = 0.1, **kwargs) -> int:
    """doc of a"""
    print(i, b, cst)
    return i


@dag
def pipe(entry: int) -> int:
    """doc of my pipeline"""
    b = abcd(entry)
    return b


def test_doc_pipeline():
    assert pipe.__doc__ == """doc of my pipeline"""


def test_name_pipeline():
    assert pipe.__name__ == "pipe"


def test_doc_operation():
    assert abcd.__doc__ == """doc of a"""


def test_name_op():
    assert abcd.__name__ == "abcd"


# TODO: add assertion for type checking after doing some research!
