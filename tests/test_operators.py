from typing import Any, cast

import pytest
from tawazi import and_, dag, not_, or_, xn


@xn
def cast_float(x: int) -> float:
    return float(x)


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


# binary operations
def test_operator_add_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        return in1 + in2

    assert pipe(7, 3) == 10


def test_operator_add_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        return in1 + 3

    assert pipe(7) == 10


def test_operator_add_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        return 3 + in1

    assert pipe(7) == 10


def test_operator_iadd_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        r = in1
        r += in2
        return r

    assert pipe(7, 3) == 10


def test_operator_iadd_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = in1
        r += 3
        return r

    assert pipe(7) == 10


def test_operator_iadd_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = 3
        r += in1
        return r

    assert pipe(7) == 10


def test_operator_sub_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        return in1 - in2

    assert pipe(7, 3) == 4


def test_operator_sub_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        return in1 - 3

    assert pipe(7) == 4


def test_operator_sub_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        return 7 - in1

    assert pipe(3) == 4


def test_operator_isub_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        r = in1
        r -= in2
        return r

    assert pipe(7, 3) == 4


def test_operator_isub_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = in1
        r -= 3
        return r

    assert pipe(7) == 4


def test_operator_isub_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = 7
        r -= in1
        return r

    assert pipe(3) == 4


def test_operator_mul_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        return in1 * in2

    assert pipe(7, 3) == 21


def test_operator_mul_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        return in1 * 3

    assert pipe(7) == 21


def test_operator_mul_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        return 7 * in1

    assert pipe(3) == 21


def test_operator_imul_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        r = in1
        r *= in2
        return r

    assert pipe(7, 3) == 21


def test_operator_imul_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = in1
        r *= 3
        return r

    assert pipe(7) == 21


def test_operator_imul_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = 7
        r *= in1
        return r

    assert pipe(3) == 21


def test_operator_matmul_uxn_uxn() -> None:
    import numpy as np

    @dag
    def pipe(in1: Any, in2: Any) -> Any:
        return in1 @ in2

    assert pipe(np.array([7, 5]), np.array([3, 4])) == 41


def test_operator_matmul_uxn_cst() -> None:
    import numpy as np

    @dag
    def pipe(in1: Any) -> Any:
        return in1 @ np.array([3, 4])

    assert pipe(np.array([7, 5])) == 41


# this fails because the operation between ndarray and right hand side is implemented for anything
#  so it doesn't raise NotImplementedError nor return NotImplemented
# def test_operator_matmul_cst_uxn() -> None:
#     import numpy as np

#     @dag
#     def pipe(in1: Any) -> Any:
#         return np.array([7, 5]) @ in1

#     assert pipe(np.array([3, 4])) == 41


def test_operator_imatmul_uxn_uxn() -> None:
    import numpy as np

    @dag
    def pipe(in1: Any, in2: Any) -> Any:
        r = in1
        r @= in2
        return r

    assert pipe(np.array([7, 5]), np.array([3, 4])) == 41


def test_operator_imatmul_uxn_cst() -> None:
    import numpy as np

    @dag
    def pipe(in1: Any) -> Any:
        r = in1
        r @= np.array([3, 4])
        return r

    assert pipe(np.array([7, 5])) == 41


# this fails because the operation between ndarray and right hand side is implemented for anything
#  so it doesn't raise NotImplementedError nor return NotImplemented
# def test_operator_imatmul_cst_uxn() -> None:
#     import numpy as np

#     @dag
#     def pipe(in1: Any) -> Any:
#         r = np.array([7, 5])
#         r @= in1
#         return r

#     assert pipe(np.array([3, 4])) == 41


def test_operator_truediv_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> float:
        return in1 / in2

    assert pipe(7, 3) == 7 / 3


def test_operator_truediv_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> float:
        return in1 / 3

    assert pipe(7) == 7 / 3


def test_operator_truediv_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> float:
        return 7 / in1

    assert pipe(3) == 7 / 3


def test_operator_itruediv_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> float:
        r = cast_float(in1)
        r /= in2
        return r

    assert pipe(7, 3) == 7 / 3


def test_operator_itruediv_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> float:
        r = cast_float(in1)
        r /= 3
        return r

    assert pipe(7) == 7 / 3


def test_operator_itruediv_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> float:
        r = 7.0
        r /= in1
        return r

    assert pipe(3) == 7 / 3


def test_operator_floordiv_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        return in1 // in2

    assert pipe(7, 3) == 2


def test_operator_floordiv_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        return in1 // 3

    assert pipe(7) == 2


def test_operator_floordiv_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        return 7 // in1

    assert pipe(3) == 2


def test_operator_ifloordiv_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        r = in1
        r //= in2
        return r

    assert pipe(7, 3) == 2


def test_operator_ifloordiv_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = in1
        r //= 3
        return r

    assert pipe(7) == 2


def test_operator_ifloordiv_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = 7
        r //= in1
        return r

    assert pipe(3) == 2


def test_operator_mod_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        return in1 % in2

    assert pipe(7, 3) == 1


def test_operator_mod_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        return in1 % 3

    assert pipe(7) == 1


def test_operator_mod_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        return 7 % in1

    assert pipe(3) == 1


def test_operator_imod_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        r = in1
        r %= in2
        return r

    assert pipe(7, 3) == 1


def test_operator_imod_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = in1
        r %= 3
        return r

    assert pipe(7) == 1


def test_operator_imod_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = 7
        r %= in1
        return r

    assert pipe(3) == 1


def test_operator_divmod_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> tuple[int, int]:
        return divmod(in1, in2)

    assert pipe(7, 3) == (2, 1)


def test_operator_divmod_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> tuple[int, int]:
        return divmod(in1, 3)

    assert pipe(7) == (2, 1)


def test_operator_divmod_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> tuple[int, int]:
        return divmod(7, in1)

    assert pipe(3) == (2, 1)


# no idivmod because builtin function with out operator equivalent


def test_operator_pow_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        return cast(int, pow(in1, in2))

    assert pipe(7, 3) == 343


def test_operator_pow_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        return pow(in1, 3)

    assert pipe(7) == 343


def test_operator_pow_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        return cast(int, pow(7, in1))

    assert pipe(3) == 343


def test_operator_pow_asterics_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        return cast(int, in1**in2)

    assert pipe(7, 3) == 343


def test_operator_pow_asterics_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        return in1**3

    assert pipe(7) == 343


def test_operator_pow_asterics_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        return cast(int, 7**in1)

    assert pipe(3) == 343


def test_operator_ipow_asterics_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        r = in1
        r **= in2
        return r

    assert pipe(7, 3) == 343


def test_operator_ipow_asterics_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = in1
        r **= 3
        return r

    assert pipe(7) == 343


def test_operator_ipow_asterics_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = 7
        r **= in1
        return r

    assert pipe(3) == 343


def test_operator_lshift_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        return in1 << in2

    assert pipe(7, 3) == 56


def test_operator_lshift_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        return in1 << 3

    assert pipe(7) == 56


def test_operator_lshift_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        return 7 << in1

    assert pipe(3) == 56


def test_operator_ilshift_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        r = in1
        r <<= in2
        return r

    assert pipe(7, 3) == 56


def test_operator_ilshift_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = in1
        r <<= 3
        return r

    assert pipe(7) == 56


def test_operator_ilshift_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = 7
        r <<= in1
        return r

    assert pipe(3) == 56


def test_operator_rshift_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        return in1 >> in2

    assert pipe(56, 3) == 7


def test_operator_rshift_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        return in1 >> 3

    assert pipe(56) == 7


def test_operator_rshift_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        return 56 >> in1

    assert pipe(3) == 7


def test_operator_irshift_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        r = in1
        r >>= in2
        return r

    assert pipe(56, 3) == 7


def test_operator_irshift_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = in1
        r >>= 3
        return r

    assert pipe(56) == 7


def test_operator_irshift_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = 56
        r >>= in1
        return r

    assert pipe(3) == 7


def test_operator_and_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        return in1 & in2

    assert pipe(7, 3) == 3


def test_operator_and_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        return in1 & 3

    assert pipe(7) == 3


def test_operator_and_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        return 7 & in1

    assert pipe(3) == 3


def test_operator_iand_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        r = in1
        r &= in2
        return r

    assert pipe(7, 3) == 3


def test_operator_iand_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = in1
        r &= 3
        return r

    assert pipe(7) == 3


def test_operator_iand_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = 7
        r &= in1
        return r

    assert pipe(3) == 3


def test_operator_xor_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        return in1 ^ in2

    assert pipe(7, 3) == 4


def test_operator_xor_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        return in1 ^ 3

    assert pipe(7) == 4


def test_operator_xor_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        return 7 ^ in1

    assert pipe(3) == 4


def test_operator_ixor_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        r = in1
        r ^= in2
        return r

    assert pipe(7, 3) == 4


def test_operator_ixor_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = in1
        r ^= 3
        return r

    assert pipe(7) == 4


def test_operator_ixor_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = 7
        r ^= in1
        return r

    assert pipe(3) == 4


def test_operator_or_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        return in1 | in2

    assert pipe(7, 3) == 7


def test_operator_or_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        return in1 | 3

    assert pipe(7) == 7


def test_operator_or_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        return 7 | in1

    assert pipe(3) == 7


def test_operator_ior_uxn_uxn() -> None:
    @dag
    def pipe(in1: int, in2: int) -> int:
        r = in1
        r |= in2
        return r

    assert pipe(7, 3) == 7


def test_operator_ior_uxn_cst() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = 3
        r |= in1
        return r

    assert pipe(7) == 7


def test_operator_ior_cst_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        r = 7
        r |= in1
        return r

    assert pipe(3) == 7


def test_operator_neg_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        return -in1

    assert pipe(123) == -123


def test_operator_pos_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        return +in1

    assert pipe(123) == 123


def test_operator_abs_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        return abs(in1)

    assert pipe(123) == 123
    assert pipe(-123) == 123


def test_operator_invert_uxn() -> None:
    @dag
    def pipe(in1: int) -> int:
        return ~in1

    assert pipe(1) == -2


def test_complicated_operator_usage() -> None:
    @dag
    def pipe(a: bool, b: bool, c: bool, d: bool) -> Any:
        return and_(a, b), a & b, or_(c, d), c | d, not_(d)

    r = pipe(True, True, True, False)

    assert r[0] is True
    assert r[1] is True
    assert r[2] is True
    assert r[2] is True
    assert r[3] is True


def test_different_types_eq() -> None:
    @dag
    def pipe(a: Any, b: Any) -> Any:
        return a == b

    assert pipe(1, 1) is True
    assert pipe(1, 2) is False
    assert pipe(1, 1.0) is True
    assert pipe(1, 2.0) is False
    assert pipe(1, "1") is False


def test_eq_and_andop() -> None:
    @dag
    def pipe(a: Any, b: Any) -> Any:
        return (a == "twinkle") & (b == "toes")

    assert pipe("twinkle", "toes") is True
    assert pipe("foo", "bar") is False
