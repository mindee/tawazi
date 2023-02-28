from typing import List

from tawazi import dag, xn


def test_imbricated_dags() -> None:
    @xn
    def op1(img: List[int]) -> int:
        return sum(img)

    @xn
    def op2(op1: int) -> int:
        return 1 - op1

    @xn
    @dag
    def op3(img: List[int]) -> int:
        return op2(op1(img))

    @xn
    def op4(img: List[int]) -> int:
        return sum(img) ** 2

    @xn
    def op5(mean: float, std: float) -> float:
        return mean + std

    @dag
    def op6(img: List[int]) -> float:
        titi = op3(img)
        toto = op4(img)

        return op5(titi, toto)

    op6([1, 2, 3, 4])
