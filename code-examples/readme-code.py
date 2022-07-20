from time import sleep
from tawazi import DAG, ExecNode


def a():
  print("Function 'a' is running", flush=True)
  sleep(1)
  return "A"


def b():
  print("Function 'b' is running", flush=True)
  sleep(1)
  return "B"


def c(a, b):
  print("Function 'c' is running", flush=True)
  print(f"Function 'c' received {a} from 'a' & {b} from 'b'", flush=True)
  return f"{a} + {b} = C"


if __name__ == "__main__":
  # Define dependencies
  # ExecNodes are defined using an id_: it has to be hashable (It can be the function itself)
  exec_nodes = [
    ExecNode(a, a, is_sequential=False),
    ExecNode(b, b, is_sequential=False),
    ExecNode(c, c, [a, b], is_sequential=False),
  ]
  g = DAG(exec_nodes, max_concurrency=2)
  g.build()
  g.execute()
  print(g.results_dict)