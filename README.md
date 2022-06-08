# tawazi

## Introduction

<!-- put a link explaining what a DAG is-->

This library helps you execute a set of functions in a **DAG** dependency structure in **parallel**.

In the context of the DAG, these functions are called `ExecNode`s.

This library supports:
* Limiting the number of "Threads" to be used
* Priority Choice of each `ExecNode`
* Per `ExecNode` choice of parallelization (i.e. An `ExecNode` is allowed to run in parallel with another `ExecNode` or not)


## Usage

```python
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
```


## Future developments
This library is still in development. Breaking changes are expected.

A couple of features will be released soon:
* support multiprocessing
* simulation of the execution using a DAG stored ledger
* support more python versions
* Try the library on windows machine
* Include python, mypy, black etc. in the README
* Disallow execution in parallel of some threads in parallel with some other threads
  * maybe by making a group of threads that are CPU bound and a group of threads that are IO bound?
* Remove dependency on networkx !?
* add line maximum columns
* decide whether to identify the ExecNode by a Hashable ID or by its own Python ID. This is breaking change and must change to 0.2.1
* support multiple return of a function!? this is rather complicated!? I have to wrap every returned value 
in an object and then decide the dependencies using that
* change the name of the library to tawazi
* the goal of this library is to run the DAG nodes in parallel and to run the same DAG in parallel in multiple threads
or to run the same ops between different DAGs with no side effects what so ever
* run subset of execnodes only
* clean the new DAG interface and document it