# tawazi
[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)
[![Python 3.9](https://img.shields.io/badge/python-3.9-blue.svg)](https://www.python.org/downloads/release/python-390/)
[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-3100/)

![Tawazi GIF](tawazi_GIF.gif)

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
