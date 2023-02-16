# type: ignore
from tawazi import dag, xn


@xn
def xn1(in1):
    return in1 + 1


@xn
def xn2(in1):
    return in1 + 2


@xn
def xn3(in1, in2):
    return in1 + in2


@dag
def pipe(in1, in2):
    r1 = xn1(in1)
    r2 = xn2(in2)
    r3 = xn3(r1, r2)
    return r1, r2, r3


def test_run_whole_dag_executor():
    executor = pipe.executor()
    r1, r2, r3 = executor(1, 2)

    assert (r1, r2, r3) == (2, 4, 6)
    assert len(executor.results) == 5


def test_run_dag_executor_multiple_times():
    # even though this is not guaranteed to work... but executors should be reusable but are not threadsafe!
    executor = pipe.executor()
    r1, r2, r3 = executor(1, 2)
    r4, r5, r6 = executor(3, 4)
    r7, r8, r9 = executor(5, 6)

    assert (r1, r2, r3, r4, r5, r6, r7, r8, r9) == (2, 4, 6, 4, 6, 10, 6, 8, 14)
    assert len(executor.results) == 5


def test_run_sub_dag_executor():
    executor = pipe.executor(target_nodes=["xn1", "xn2"])
    r1, r2, r3 = executor(1, 2)
    assert (r1, r2, r3) == (2, 4, None)
