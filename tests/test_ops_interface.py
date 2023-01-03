#  type: ignore
from logging import Logger
from typing import Union

import pytest

from tawazi import op, to_dag

"""integration test"""

logger = Logger(name="mylogger", level="ERROR")


def test_ops_interface():
    @op
    def a():
        logger.debug("ran a")
        return "a"

    @op
    def b(a):
        logger.debug(f"a is {a}")
        logger.debug("ran b")
        return "b"

    @op
    def c(a):
        logger.debug(f"a is {a}")
        logger.debug("ran c")
        return "c"

    @op
    def d(b, c, third_argument: Union[str, int] = 1234, fourth_argument=6789):
        logger.debug(f"b is {b}")
        logger.debug(f"c is {c}")
        logger.debug(f"third argument is {third_argument}")
        pytest.third_argument = third_argument
        logger.debug(f"fourth argument is {fourth_argument}")
        pytest.fourth_argument = fourth_argument
        logger.debug("ran d")
        # logger.debug(f"ran d {some_constant} {keyworded_arg}")
        return "d"

    @to_dag
    def my_custom_dag():
        vara = a()
        varb = b(vara)
        varc = c(vara)
        _vard = d(varb, c=varc, fourth_argument=1111)

    @op
    def e():
        logger.debug("ran e")
        return "e"

    @op
    def f(a, b, c, d, e):
        logger.debug(f"a is {a}")
        logger.debug(f"b is {b}")
        logger.debug(f"c is {c}")
        logger.debug(f"d is {d}")
        logger.debug(f"e is {e}")
        logger.debug("ran f")

    @to_dag
    def my_other_custom_dag():
        vara = a()
        varb = b(vara)
        varc = c(vara)
        vard = d(varb, c=varc, fourth_argument=2222, third_argument="blabla")
        vare = e()
        _varf = f(vara, varb, varc, vard, vare)

    d1 = my_custom_dag()
    logger.debug("\n1st execution of dag")
    d1.execute()
    assert pytest.third_argument == 1234
    assert pytest.fourth_argument == 1111
    logger.debug("\n2nd execution of dag")
    d1.execute()
    assert pytest.third_argument == 1234
    assert pytest.fourth_argument == 1111

    d2 = my_other_custom_dag()
    logger.debug("\n1st execution of other dag")
    d2.execute()
    assert pytest.third_argument == "blabla"
    assert pytest.fourth_argument == 2222
    logger.debug("\n2nd execution of other dag")
    d2.execute()
    assert pytest.third_argument == "blabla"
    assert pytest.fourth_argument == 2222

    logger.debug("\n3rd execution of dag")
    d1.execute()
    assert pytest.third_argument == 1234
    assert pytest.fourth_argument == 1111
