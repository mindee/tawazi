from logging import Logger
from typing import Union

from tawazi import dag, xn

logger = Logger(name="mylogger", level="ERROR")

glb_third_argument = None
glb_fourth_argument = None


def test_ops_interface() -> None:
    @xn
    def a() -> str:
        logger.debug("ran a")
        return "a"

    @xn
    def b(a: str) -> str:
        logger.debug(f"a is {a}")
        logger.debug("ran b")
        return "b"

    @xn
    def c(a: str) -> str:
        logger.debug(f"a is {a}")
        logger.debug("ran c")
        return "c"

    @xn
    def d(
        b: str, c: str, third_argument: Union[str, int] = 1234, fourth_argument: int = 6789
    ) -> str:
        logger.debug(f"b is {b}")
        logger.debug(f"c is {c}")
        logger.debug(f"third argument is {third_argument}")
        global glb_third_argument, glb_fourth_argument
        glb_third_argument = third_argument
        logger.debug(f"fourth argument is {fourth_argument}")
        glb_fourth_argument = fourth_argument
        logger.debug("ran d")
        # logger.debug(f"ran d {some_constant} {keyworded_arg}")
        return "d"

    @dag
    def my_custom_dag() -> None:
        vara = a()
        varb = b(vara)
        varc = c(vara)
        _vard = d(varb, c=varc, fourth_argument=1111)

    @xn
    def e() -> str:
        logger.debug("ran e")
        return "e"

    @xn
    def f(a: str, b: str, c: str, d: str, e: str) -> None:
        logger.debug(f"a is {a}")
        logger.debug(f"b is {b}")
        logger.debug(f"c is {c}")
        logger.debug(f"d is {d}")
        logger.debug(f"e is {e}")
        logger.debug("ran f")

    @dag
    def my_other_custom_dag() -> None:
        vara = a()
        varb = b(vara)
        varc = c(vara)
        vard = d(varb, c=varc, fourth_argument=2222, third_argument="blabla")
        vare = e()
        _varf = f(vara, varb, varc, vard, vare)

    logger.debug("\n1st execution of dag")
    my_custom_dag()
    assert glb_third_argument == 1234
    assert glb_fourth_argument == 1111
    logger.debug("\n2nd execution of dag")
    my_custom_dag()
    assert glb_third_argument == 1234
    assert glb_fourth_argument == 1111

    logger.debug("\n1st execution of other dag")
    my_other_custom_dag()
    assert glb_third_argument == "blabla"
    assert glb_fourth_argument == 2222
    logger.debug("\n2nd execution of other dag")
    my_other_custom_dag()
    assert glb_third_argument == "blabla"
    assert glb_fourth_argument == 2222

    logger.debug("\n3rd execution of dag")
    my_custom_dag()
    assert glb_third_argument == 1234
    assert glb_fourth_argument == 1111
