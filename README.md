# tawazi

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


## Future developments
This library is still in development. Breaking changes are expected.

A couple of features will be released soon:
* an example of running a set of calculations with the DAG and without the DAG
  * we can show that using tawazi is transparent and one just has to remove the decorators! Then everything is back to normal
* support multiprocessing
* simulation of the execution using a DAG stored ledger
* support more python versions
* Try the library on Windows machine
* Include python, mypy, black etc. in the README
* Disallow execution in parallel of some threads in parallel with some other threads
  * maybe by making a group of threads that are CPU bound and a group of threads that are IO bound?
* Remove dependency on networkx !?
* add line maximum columns
* decide whether to identify the ExecNode by a Hashable ID or by its own Python ID. This is breaking change and must change to 0.2.1
* support multiple return of a function!? this is rather complicated!? I have to wrap every returned value
in an object and then decide the dependencies using that
* the goal of this library is to run the DAG nodes in parallel and to run the same DAG in parallel in multiple threads
or to run the same ops between different DAGs with no side effects whatsoever
* run subset of exec nodes only
* clean the new DAG interface and document it
* document dagster interface and correct the tests
* put documentation about different cases where it is advantageous to use it
  * in methods not only in functions
  * in a gunicorn application
  * for getting information from multiple resources
* support constants by resolving the error in the tests
* put link to code on GitHub
* Remove the global object and maybe replace it with an attribute to the creating function
