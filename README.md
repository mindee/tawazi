# tawazi
[![Python 3.7](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-370/)
[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)
[![Python 3.9](https://img.shields.io/badge/python-3.9-blue.svg)](https://www.python.org/downloads/release/python-390/)
[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-3100/)
[![CodeFactor](https://www.codefactor.io/repository/github/mindee/tawazi/badge)](https://www.codefactor.io/repository/github/mindee/tawazi)
[![Downloads](https://img.shields.io/pypi/dm/tawazi)](https://pypi.org/project/tawazi/)

![Tawazi GIF](documentation/tawazi_GIF.gif)

## Introduction

<!-- TODO: put a link explaining what a DAG is-->

The tawazi library enables **parallel** execution of functions in a [DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph) dependency structure.
This library satisfies the following:
* Stable, robust, well tested
* lightweight
* Thread Safety
* Low to no dependencies
* Legacy Python versions support (in the future)
* pypy support (in the future)

In the context of tawazi, the computation sequence to be run in parallel is referred to as DAG and the functions that must run in parallel are called `ExecNode`s.

This library supports:
* Limitation of the number of "Threads"
* Priority Choice of each `ExecNode`
* Per `ExecNode` choice of parallelization (i.e. An `ExecNode` is allowed to run in parallel with other `ExecNode`s or not)

**Note**: The library is still at an [advanced state of development](#future-developments). Your contributions are highly welcomed.


## Usage
```python

# type: ignore
from time import sleep, time
from tawazi import op, to_dag

@op
def a():
    print("Function 'a' is running", flush=True)
    sleep(1)
    return "A"

@op
def b():
    print("Function 'b' is running", flush=True)
    sleep(1)
    return "B"

@op
def c(a, b):
    print("Function 'c' is running", flush=True)
    print(f"Function 'c' received {a} from 'a' & {b} from 'b'", flush=True)
    return f"{a} + {b} = C"

@to_dag(max_concurrency=2)
def deps_describer():
  result_a = a()
  result_b = b()
  result_c = c(result_a, result_b)

if __name__ == "__main__":

  t0 = time()
  # executing the dag takes a single line of code
  deps_describer().execute()
  execution_time = time() - t0
  assert execution_time < 1.5
  print(f"Graph execution took {execution_time:.2f} seconds")

```

## Advanced Usage

```python

# type: ignore
from time import sleep, time
from tawazi import op, to_dag

@op
def a():
    print("Function 'a' is running", flush=True)
    sleep(1)
    return "A"

# optionally configure each op using the decorator:
# is_sequential = True to prevent op from running in parallel with other ops
# priority to choose the op in the next execution phase
@op(is_sequential=True, priority=10)
def b():
    print("Function 'b' is running", flush=True)
    sleep(1)
    return "B"

@op
def c(a, arg_b):
    print("Function 'c' is running", flush=True)
    print(f"Function 'c' received {a} from 'a' & {arg_b} from 'b'", flush=True)
    return f"{a} + {arg_b} = C"

# optionally customize the DAG
@to_dag(max_concurrency=2, behavior="strict")
def deps_describer():
  result_a = a()
  result_b = b()
  result_c = c(result_a, result_b)

if __name__ == "__main__":

  t0 = time()
  # the dag instance is reusable.
  # This is recommended if you want to do the same computation multiple times
  dag = deps_describer()
  results_1 = dag.execute()
  execution_time = time() - t0
  print(f"Graph execution took {execution_time:.2f} seconds")

  # debugging the code using `dag.safe_execute()` is easier
  # because the execution doesn't go through the Thread pool
  results_2 = dag.safe_execute()

  # you can look throught the results of each operation like this:
  for my_op in [a, b, c]:
    assert results_1[my_op.id].result == results_2[my_op.id].result

```


## Name explanation
The libraries name is inspired from the arabic word تَوَازٍ which means parallel.

## Building the doc
The general documentation has no dedicated space at the moment and is being hosted offline (on your machine).
Expect future developments on this side as well. To build it, simply run `mkdocs build` and `mkdocs serve` at the root of the repository.

## Future developments
*This library is still in development. Breaking changes are expected.
