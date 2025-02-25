from tawazi import dag, xn


@xn
def op1(img: list[int]) -> int:
    return sum(img)


@xn
def op2(op1: int) -> int:
    return 1 - op1


@xn
def op4(img: list[int]) -> int:
    return sum(img) ** 2


@xn
def op5(mean: float, std: float) -> float:
    return mean + std


def test_imbricated_dags() -> None:
    @xn
    @dag
    def op3(img: list[int]) -> int:
        return op2(op1(img))

    @dag
    def op6(img: list[int]) -> float:
        titi = op3(img)
        toto = op4(img)

        return op5(titi, toto)

    assert op6([1, 2, 3, 4]) == 91
