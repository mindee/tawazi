from logging import Logger
from typing import Union

from tawazi import xn

logger = Logger(name="mylogger", level="ERROR")

glb_third_argument = None
glb_fourth_argument = None


class MyClass:
    @xn
    def a(self) -> str:
        logger.debug("ran a")
        return "a"

    @xn
    def b(self, a: str) -> str:
        logger.debug(f"a is {a}")
        logger.debug("ran b")
        return "b"

    @xn
    def c(self, a: str) -> str:
        logger.debug(f"a is {a}")
        logger.debug("ran c")
        return "c"

    @xn
    def d(
        self, b: str, c: str, third_argument: Union[str, int] = 1234, fourth_argument: int = 6789
    ) -> str:
        logger.debug(f"b is {b}")
        logger.debug(f"c is {c}")
        logger.debug(f"third argument is {third_argument}")
        global glb_third_argument, glb_fourth_argument
        glb_third_argument = third_argument
        logger.debug(f"fourth argument is {fourth_argument}")
        glb_fourth_argument = fourth_argument
        logger.debug("ran d")
        return "d"


#     @dag
#     def my_custom_dag(self) -> None:
#         vara = self.a()
#         varb = self.b(vara)
#         varc = self.c(vara)
#         _vard = self.d(varb, c=varc, fourth_argument=1111)


# def test_ops_interface() -> None:
#     c = MyClass()

#     d1 = c.my_custom_dag
#     logger.debug("\n1st execution of dag")
#     d1.execute()
#     assert glb_third_argument == 1234
#     assert glb_fourth_argument == 1111
#     logger.debug("\n2nd execution of dag")
#     d1.execute()


# The use case for this feature is the following:

# The user has a Class with an instance that contains a lof of methods and attributes.
# This class contains a very complicated function that can benefit from parallelization.
# The user wants to run the dag multiple times and get the same results...
#     he is responsible for the modifications that any function might do on the instance...
#     He should be warned about the parallelization dangers when dealing with shared data (in this case self!)
# The user (in the best case scenario) will only exchange mutable data between the methods via the parameters
# This will ensure the dependency of execution will be respected!
# TODO: do some advanced tests on this case!
