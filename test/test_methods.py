from typing import Union
from tawazi import op, to_dag
import pytest
from logging import Logger

logger = Logger(name="mylogger", level="ERROR")


class MyClass():
    @op
    def a(self, ):
        logger.debug('ran a')
        return "a"

    @op
    def b(self, a):
        logger.debug(f"a is {a}")
        logger.debug('ran b')
        return "b"

    @op
    def c(self, a):
        logger.debug(f"a is {a}")
        logger.debug('ran c')
        return 'c'

    @op
    def d(self, b, c, third_argument: Union[str, int] = 1234, fourth_argument=6789):
        logger.debug(f"b is {b}")
        logger.debug(f"c is {c}")
        logger.debug(f'third argument is {third_argument}')
        pytest.third_argument = third_argument
        logger.debug(f'fourth argument is {fourth_argument}')
        pytest.fourth_argument = fourth_argument
        logger.debug(f"ran d")
        # logger.debug(f"ran d {some_constant} {keyworded_arg}")
        return "d"

    @to_dag
    def my_custom_dag(self):
        vara = self.a()
        varb = self.b(vara)
        varc = self.c(vara)
        vard = self.d(varb, c=varc, fourth_argument=1111)


def test_ops_interface():
    c = MyClass()

    d1 = c.my_custom_dag()
    logger.debug(f"\n1st execution of dag")
    d1.execute()
    assert pytest.third_argument == 1234
    assert pytest.fourth_argument == 1111
    logger.debug(f"\n2nd execution of dag")
    d1.execute()
