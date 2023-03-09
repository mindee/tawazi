from time import sleep

from networkx import NetworkXUnfeasible
from tawazi import DAG, ErrorStrategy
from tawazi.node import ExecNode, UsageExecNode

T = 0.1


def a(c: str) -> str:
    sleep(T)
    return "a"


def b(a: str) -> str:
    sleep(T)
    return a + "b"


def c(b: str) -> str:
    sleep(T)
    return b + "c"


en_a = ExecNode(a.__name__, a, [], is_sequential=True)
en_b = ExecNode(b.__name__, b, [UsageExecNode(en_a.id)], priority=2, is_sequential=False)
en_c = ExecNode(c.__name__, c, [UsageExecNode(en_a.id)], priority=1, is_sequential=False)
en_a.args = [UsageExecNode(en_c.id)]

list_exec_nodes = [en_a, en_b, en_c]
node_dict = {xn.id: xn for xn in list_exec_nodes}


def test_circular_deps() -> None:
    try:
        DAG(node_dict, [], [], 2, behavior=ErrorStrategy.strict)
    except NetworkXUnfeasible:
        pass


# @op
# def n1(img):
#     print(len(img))
#     return len(img)

# @op
# def n2(n: int):
#     print("the length is ", n)
#     return n

# @to_dag
# def pipeline(img):

#     # img ?
#     _len = n1(img)
#     n = n2(_len)

#     # ?
#     return n

# # this is the best option!
# pipeline(img)

# # second option if 1st isn't possible
# my_dag = pipeline.make_dag()


# def autre_fonction(img: List[int]):
#     # called via Product.run()
#     returned_value = my_dag(img)
