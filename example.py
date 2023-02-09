# flake8: noqa
# type: ignore

from typing import Union

import pytest

from tawazi import dag, xn


@xn
def a():
    print("ran a")
    return "a"


@xn
def b(a):
    print(f"a is {a}")
    print("ran b")
    return "b"


@xn
def c(a):
    print(f"a is {a}")
    print("ran c")
    return "c"


@xn
def d(b, c, third_argument: Union[str, int] = 1234, fourth_argument=6789):
    print(f"b is {b}")
    print(f"c is {c}")
    print(f"third argument is {third_argument}")
    pytest.third_argument = third_argument
    print(f"fourth argument is {fourth_argument}")
    pytest.fourth_argument = fourth_argument
    print("ran d")
    # print(f"ran d {some_constant} {keyworded_arg}")
    return "d"


@dag
def my_custom_dag():
    vara = a()
    varb = b(vara)
    varc = c(vara)
    _vard = d(varb, c=varc, fourth_argument=1111)


@xn
def e():
    print("ran e")
    return "e"


@xn
def f(a, b, c, d, e):
    print(f"a is {a}")
    print(f"b is {b}")
    print(f"c is {c}")
    print(f"d is {d}")
    print(f"e is {e}")
    print("ran f")


@dag
def my_other_custom_dag():
    vara = a()
    varb = b(vara)
    varc = c(vara)
    vard = d(varb, c=varc, fourth_argument=2222, third_argument="blabla")
    vare = e()
    _varf = f(vara, varb, varc, vard, vare)


d1 = my_custom_dag
print("\n1st execution of dag")
d1()
assert pytest.third_argument == 1234
assert pytest.fourth_argument == 1111
print("\n2nd execution of dag")
d1()

d2 = my_other_custom_dag
print("\n1st execution of other dag")
d2()
assert pytest.third_argument == "blabla"
assert pytest.fourth_argument == 2222
print("\n2nd execution of other dag")
d2()

print("\n3rd execution of dag")
d1()
assert pytest.third_argument == 1234
assert pytest.fourth_argument == 1111
