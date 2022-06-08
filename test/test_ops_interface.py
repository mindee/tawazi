from typing import Union
from tawazi import op, to_dag
import pytest


def test_ops_interface():
    @op
    def a():
        print('ran a')
        return "a"

    @op
    def b(a):
        print(f"a is {a}")
        print('ran b')
        return "b"

    @op
    def c(a):
        print(f"a is {a}")
        print('ran c')
        return 'c'

    @op
    def d(b, c, third_argument: Union[str, int]=1234, fourth_argument=6789):
        print(f"b is {b}")
        print(f"c is {c}")
        print(f'third argument is {third_argument}')
        pytest.third_argument = third_argument
        print(f'fourth argument is {fourth_argument}')
        pytest.fourth_argument = fourth_argument
        print(f"ran d")
        # print(f"ran d {some_constant} {keyworded_arg}")
        return "d"

    @to_dag
    def my_custom_dag():
        vara = a()
        varb = b(vara)
        varc = c(vara)
        vard = d(varb, c=varc, fourth_argument=1111)

    @op
    def e():
        print("ran e")
        return "e"

    @op
    def f(a, b, c, d, e):
        print(f"a is {a}")
        print(f"b is {b}")
        print(f"c is {c}")
        print(f"d is {d}")
        print(f"e is {e}")
        print("ran f")

    @to_dag
    def my_other_custom_dag():
        vara = a()
        varb = b(vara)
        varc = c(vara)
        vard = d(varb, c=varc, fourth_argument=2222, third_argument='blabla')
        vare = e()
        varf = f(vara, varb, varc, vard, vare)

    d1 = my_custom_dag()
    print(f"\n1st execution of dag")
    d1.execute()
    assert pytest.third_argument == 1234
    assert pytest.fourth_argument == 1111
    print(f"\n2nd execution of dag")
    d1.execute()

    d2 = my_other_custom_dag()
    print("\n1st execution of other dag")
    d2.execute()
    assert pytest.third_argument == 'blabla'
    assert pytest.fourth_argument == 2222
    print("\n2nd execution of other dag")
    d2.execute()

    print(f"\n3rd execution of dag")
    d1.execute()
    assert pytest.third_argument == 1234
    assert pytest.fourth_argument == 1111
