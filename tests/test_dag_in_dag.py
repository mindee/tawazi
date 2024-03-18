from tawazi import dag, xn


@xn
def func(arg1, arg2):
    return arg1 + arg2


def test_dag_in_dag():
    @dag
    def dag2(arg1, arg2):
        return func(arg1, arg2)

    @dag
    def dag1(arg1, arg2):
        return dag2(arg1, arg2)

    assert dag1(1, 2) == 3
    assert dag1(2, 3) == 5


def test_dag_in_dag_ids_conflict():
    pass
