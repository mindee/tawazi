from typing import Optional, Tuple, TypeVar

from tawazi import and_, dag, xn

T = TypeVar("T")


@xn
def stub(x: T) -> T:
    return x


def test_active_cst() -> None:
    @dag
    def pipe() -> Optional[int]:
        return stub(1, twz_active=True)  # type: ignore[call-arg]

    assert pipe() == 1

    @dag
    def pipe2() -> Optional[int]:
        return stub(1, twz_active=False)  # type: ignore[call-arg]

    assert pipe2() is None


def test_active_uxn() -> None:
    @dag
    def pipe(x: int) -> Tuple[int, Optional[int]]:
        # if x is Truthy the value is returned
        y = stub(x, twz_active=x)  # type: ignore[call-arg]
        return x, y

    assert pipe(1) == (1, 1)
    assert pipe(0) == (0, None)


def test_active_uxn_eq_str() -> None:
    @dag
    def pipe(x: str, y: str) -> Tuple[str, str, Optional[str], bool]:
        b = and_(x == "twinkle", y == "toes")
        z = stub(x, twz_active=b)  # type: ignore[call-arg]
        return x, y, z, b

    assert pipe("twinkle", "toes") == ("twinkle", "toes", "twinkle", True)
    assert pipe("hello", "world") == ("hello", "world", None, False)


def test_active_uxn_eq_int() -> None:
    @dag
    def pipe(x: int, y: int) -> Tuple[int, int, Optional[int], bool]:
        b = and_(x == 1, y == 2)
        z = stub(x, twz_active=b)  # type: ignore[call-arg]
        return x, y, z, b

    assert pipe(1, 2) == (1, 2, 1, True)
    assert pipe(3, 4) == (3, 4, None, False)


def test_active_operations_in_graph() -> None:
    @dag
    def pipe(x: int, y: int) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[float]]:
        r1 = stub(x + y, twz_active=x + y > 0)  # type: ignore[call-arg]
        r2 = stub(x - y, twz_active=x - y > 0)  # type: ignore[call-arg]
        r3 = stub(x * y, twz_active=x * y > 0)  # type: ignore[call-arg]
        r4 = stub(x / y, twz_active=x / y > 0)  # type: ignore[call-arg]

        return r1, r2, r3, r4

    assert pipe(-1, -1) == (None, None, 1, 1.0)
    assert pipe(-1, 1) == (None, None, None, None)


def test_active_impossible_cases_graph() -> None:
    @dag
    def pipe(
        x: int, y: int
    ) -> Tuple[
        Optional[int], Optional[int], Optional[int], Optional[int], Optional[int], Optional[int]
    ]:
        a = stub(x, twz_active=x < 0)  # type: ignore[call-arg]
        b = stub(y, twz_active=y > 0)  # type: ignore[call-arg]
        c = stub(x + 1, twz_active=x > 0)  # type: ignore[call-arg]
        d = stub(x + y, twz_active=and_(a, b))  # type: ignore[call-arg]

        imposs_1 = stub(a + c, twz_active=and_(a, c))  # type: ignore[call-arg]

        imposs_2 = stub(a + b, twz_active=and_(c, d))  # type: ignore[call-arg]

        return a, b, c, d, imposs_1, imposs_2

    assert pipe(+1, +1) == (None, +1, +2, None, None, None)
    assert pipe(+1, -1) == (None, None, +2, None, None, None)
    assert pipe(-1, +1) == (-1, +1, None, 0, None, None)
    assert pipe(-1, -1) == (-1, None, None, None, None, None)


def test_active_with_setup_node() -> None:
    setop_var = 0

    @xn(setup=True)
    def setop(k: int = 1234) -> int:
        nonlocal setop_var
        setop_var += 1
        return k + 1

    @dag
    def pipe() -> Optional[int]:
        x = False
        return setop(twz_active=x)  # type: ignore[call-arg]

    assert pipe() is None
    assert setop_var == 0

    @dag
    def pipe2() -> Optional[int]:
        x = True
        return setop(twz_active=x)  # type: ignore[call-arg]

    assert pipe2() == 1235
    assert setop_var == 1


def test_active_with_exclude_node() -> None:
    @dag
    def pipe() -> Optional[int]:
        x = True
        return stub(1, twz_active=x)  # type: ignore[call-arg]

    exec_ = pipe.executor(exclude_nodes=[stub])
    assert exec_() is None
