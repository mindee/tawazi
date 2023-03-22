from tawazi import dag, xn


@xn
def a(in1):
    return in1


@xn
def b(in1):
    return not in1


def test_operator_eq_uxn_uxn():
    @dag
    def pipe(in1):
        va = a(in1)

        return va == va

    assert pipe(1)
    assert pipe(0)


def test_opereator_eq_uxn_cst():
    @dag
    def pipe(in1):
        va = a(in1)

        return va == 1

    assert pipe(1)
    assert not pipe(0)
