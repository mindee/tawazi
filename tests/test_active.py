import pytest
from tawazi import and_, dag, xn


@xn
def stub(x):
    return x


def test_active_cst() -> None:
    @dag
    def pipe():
        return stub(1, twz_active=True)

    assert pipe() == 1

    @dag
    def pipe():
        return stub(1, twz_active=False)

    assert pipe() is None


def test_active_uxn() -> None:
    @dag
    def pipe(x):
        # if x is Truthy the value is returned
        y = stub(x, twz_active=x)
        return x, y

    assert pipe(1) == (1, 1)
    assert pipe(0) == (0, None)


def test_active_uxn_eq_str() -> None:
    @dag
    def pipe(x, y):
        b = and_(x == "twinkle", y == "toes")
        z = stub(x, twz_active=b)
        return x, y, z, b

    assert pipe("twinkle", "toes") == ("twinkle", "toes", "twinkle", True)
    assert pipe(1, 2) == (1, 2, None, False)


def test_active_uxn_eq_int() -> None:
    @dag
    def pipe(x, y):
        b = and_(x == 1, y == 2)
        z = stub(x, twz_active=b)
        return x, y, z, b

    assert pipe(1, 2) == (1, 2, 1, True)
    assert pipe(3, 4) == (3, 4, None, False)


def test_active_operations_in_graph() -> None:
    @dag
    def pipe(x, y):
        r1 = stub(x + y, twz_active=x + y > 0)
        r2 = stub(x - y, twz_active=x - y > 0)
        r3 = stub(x * y, twz_active=x * y > 0)
        r4 = stub(x / y, twz_active=x / y > 0)

        return r1, r2, r3, r4

    assert pipe(-1, -1) == (None, None, 1, 1)
    assert pipe(-1, 1) == (None, None, None, None)


def test_active_impossible_cases_graph() -> None:
    @dag
    def pipe(x, y):
        a = stub(x, twz_active=x < 0)
        b = stub(y, twz_active=y > 0)
        c = stub(x + 1, twz_active=x > 0)
        d = stub(x + y, twz_active=a & b)

        imposs_1 = stub(a + c, twz_active=a & c)

        imposs_2 = stub(a + b, twz_active=c & d)

        return a, b, c, d, imposs_1, imposs_2

    assert pipe(+1, +1) == (None, +1, +2, None, None, None)
    assert pipe(+1, -1) == (None, None, +2, None, None, None)
    assert pipe(-1, +1) == (-1, +1, None, 0, None, None)
    assert pipe(-1, -1) == (-1, None, None, None, None, None)


def test_active_with_setup_node() -> None:
    pytest.setop = 0

    @xn(setup=True)
    def setop(k: int = 1234):
        pytest.setop += 1
        return k + 1

    @dag
    def pipe():
        x = False
        return setop(twz_active=x)

    assert pipe() is None
    assert pytest.setop == 0

    @dag
    def pipe():
        x = True
        return setop(twz_active=x)

    assert pipe() == 1235
    assert pytest.setop == 1


def test_active_with_exclude_node() -> None:
    @dag
    def pipe():
        x = True
        return stub(1, twz_active=x)

    exec_ = pipe.executor(exclude_nodes=[stub])
    assert exec_() is None
