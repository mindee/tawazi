from tawazi import op, to_dag

@op(priority=1)
def a():
    return "a"

@op(priority=1)
def b(a):
    print(f"{a=}")
    return "b"

@op(priority=1)
def c(a):
    print(f"{a=}")
    return "c"

@op(priority=1)
def d(b):
    print(f"{b=}")
    return "d"

@op(priority=1)
def e():
    print(f"{e=}")

@to_dag
def dependency_describer():
    _a = a()
    _b = b(_a)
    _c = c(_a)
    _d = d(_b)
    _e = e()

def test_compound_priority():
    dag = dependency_describer()

    assert dag.node_dict_by_name["a"].compound_priority == 4
    assert dag.node_dict_by_name["b"].compound_priority == 2
    assert dag.node_dict_by_name["c"].compound_priority == 1
    assert dag.node_dict_by_name["d"].compound_priority == 1
    assert dag.node_dict_by_name["e"].compound_priority == 1
