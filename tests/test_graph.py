#  type: ignore
from time import sleep

from networkx import NetworkXUnfeasible

from tawazi import DAG, ErrorStrategy
from tawazi.node import ExecNode

"""unit test"""

T = 0.1


def a(c):
    sleep(T)
    return "a"


def b(a):
    sleep(T)
    return a + "b"


def c(b):
    sleep(T)
    return b + "c"


en_a = ExecNode(id=a.__name__, exec_function=a, args=[], is_sequential=True)
en_b = ExecNode(id=b.__name__, exec_function=b, args=[en_a], priority=2, is_sequential=False)
en_c = ExecNode(id=c.__name__, exec_function=c, args=[en_a], priority=1, is_sequential=False)
en_a.args = [en_c]

list_exec_nodes = [en_a, en_b, en_c]


def test_circular_deps():
    try:
        g = DAG(exec_nodes=list_exec_nodes, max_concurrency=2, behavior=ErrorStrategy.strict)
    except NetworkXUnfeasible:
        pass


"""
@op
def n1(img):
    print(len(img))
    return len(img)

@op
def n2(n: int):
    print("the length is ", n)
    return n

@to_dag
def pipeline(img):

    # img ?
    _len = n1(img)
    n = n2(_len)

    # ?
    return n

# this is the best option!
pipeline(img)

# second option if 1st isn't possible
my_dag = pipeline.make_dag()


def autre_fonction(img: List[int]):
    # called via Product.run()
    returned_value = my_dag(img)

"""
