from typing import Any

import pytest
from tawazi import dag, xn


@xn
def a(in1: int) -> int:
    return in1


@xn
def b(in1: int) -> int:
    return not in1


# lt
def test_operator_lt_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> bool:
        return in1 < in2

    assert pipe(1, 2) is True
    assert pipe(2, 2) is False


def test_operator_lt_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> bool:
        return in1 < 2

    assert pipe(1) is True


def test_operator_lt_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> bool:
        return 1 < in1

    assert pipe(2) is True


# le
def test_operator_le_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> bool:
        return in1 <= in2

    assert pipe(1, 2) is True
    assert pipe(2, 2) is True


def test_operator_le_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> bool:
        return in1 <= 2

    assert pipe(1) is True


def test_operator_le_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> bool:
        return 1 <= in1

    assert pipe(2) is True


# eq
def test_operator_eq_uxn_uxn() -> None:
    @dag
    def pipe(in1: int) -> bool:
        va = a(in1)

        return va == va

    assert pipe(1) is True
    assert pipe(0) is True


def test_opereator_eq_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> bool:
        va = a(in1)

        return va == 1

    assert pipe(1) is True
    assert pipe(0) is False


def test_opereator_eq_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> bool:
        va = a(in1)

        return 1 == va

    assert pipe(1) is True
    assert pipe(0) is False


# ne
def test_operator_ne_uxn_uxn() -> None:
    @dag
    def pipe(in1: int) -> bool:
        va = a(in1)

        return va != va

    assert pipe(1) is False
    assert pipe(0) is False


def test_opereator_ne_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> bool:
        va = a(in1)

        return va != 1

    assert pipe(1) is False
    assert pipe(0) is True


def test_opereator_ne_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> bool:
        va = a(in1)

        return 1 != va

    assert pipe(1) is False
    assert pipe(0) is True


# gt
def test_operator_gt_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> bool:
        return in1 > in2

    assert pipe(1, 2) is False
    assert pipe(2, 2) is False
    assert pipe(3, 2) is True


def test_operator_gt_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> bool:
        return in1 > 2

    assert pipe(1) is False


def test_operator_gt_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> bool:
        return 1 > in1

    assert pipe(2) is False


# ge
def test_operator_ge_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> bool:
        return in1 >= in2

    assert pipe(1, 2) is False
    assert pipe(2, 2) is True


def test_operator_ge_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> bool:
        return in1 >= 2

    assert pipe(1) is False


def test_operator_ge_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> bool:
        return 1 >= in1

    assert pipe(2) is False


# bool
def test_operator_bool_uxn() -> None:
    with pytest.raises(NotImplementedError):

        @dag
        def pipe(in1: Any) -> bool:
            return bool(in1)


def test_operator_contains_uxn() -> None:
    with pytest.raises(NotImplementedError):

        @dag
        def pipe(in1: Any) -> bool:
            return 1 in in1


# TODO: complicated case (multiple and or etc in same expression)
