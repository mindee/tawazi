# tawazi
[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)
[![Python 3.9](https://img.shields.io/badge/python-3.9-blue.svg)](https://www.python.org/downloads/release/python-390/)
[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-3100/)

![Tawazi GIF](documentation/tawazi_GIF.gif)

## Introduction

<!-- TODO: put a link explaining what a DAG is-->

The tawazi library enables **parallel** execution of functions in a [DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph) dependency structure.
This library satisfies the following:
* Stable, robust, well tested
* lightweight
* Thread Safety
* Low to no dependencies
* Legacy Python versions support
* pypy support

In the context of the DAG, these functions are called `ExecNode`s.

This library supports:
* Limiting the number of "Threads" to be used
* Priority Choice of each `ExecNode`
* Per `ExecNode` choice of parallelization (i.e. An `ExecNode` is allowed to run in parallel with another `ExecNode` or not)

**Note**: The library is still at an [advanced state of development](#future-developments). Your contributions are highly welcomed.

## Usage

```python
#  type: ignore
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
    g.execute()
```

## Name explanation
The libraries name is inspired from the arabic word تَوَازٍ which means parallel.

## Building the doc
The general documentation has no dedicated space at the moment and is being hosted offline (on your machine).
Expect future developments on this side as well. To build it, simply run
```shell
mkdocs build
mkdocs serve
```
at the root of the repository

## Future developments
*This library is still in development. Breaking changes are expected.*

### Soon to be released
A couple of features will be released soon:
* handle problems when calling `ExecNodes` wrongly.
  * (for example when using *args as parameters but only **kwargs are provided).
  * Calling `ExecNodes` must be similar to calling the original function (must imitate the same signature otherwise raise the correct exeception).
* support mixing ExecNodes and non `ExecNodes` functions.
* test the case where `ExecNodes` are stored in a list and then passed via * operator.
* add feature to turn off a set of nodes in the graph.
* an example of running a set of calculations with the `DAG` and without the `DAG`.
  * we can show that using tawazi is transparent and one just has to remove the decorators. Then everything is back to normal.
* Include python, mypy, black etc. in the `README`.
* decide whether to identify the `ExecNode` by a Hashable ID or by its own Python ID. This is breaking change and must change to 0.2.1.
* support multiple return of a function!? this is rather complicated. We'll have to wrap every returned value.
in an object and then decide the dependencies using that.
* support constants by resolving the error in the tests.
* Remove the global object and maybe replace it with an attribute to the creating function.
* improve the graph dependency rendering on the console.
* change the behavior of the execution according to the return value of the dagger function:
  * return all the results of the execution of all returned `ExecNodes`.
  * also return all the results just like it is being done at the moment.

### Features to be discussed
* support multiprocessing.
* simulation of the execution using a `DAG` stored ledger.
* Disallow execution in parallel of some threads in parallel with some other threads.
  * maybe by making a group of threads that are CPU bound and a group of threads that are IO bound ?
* Remove dependency on networkx to make `tawazi` a standalone package.
