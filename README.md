# tawazi
[![Python 3.7](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-370/)
[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)
[![Python 3.9](https://img.shields.io/badge/python-3.9-blue.svg)](https://www.python.org/downloads/release/python-390/)
[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-3100/)
[![CodeFactor](https://www.codefactor.io/repository/github/mindee/tawazi/badge)](https://www.codefactor.io/repository/github/mindee/tawazi)

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
  deps_describer()
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
  res_a = a()
  res_b = b()
  res_c = c(res_a, res_b)
  return res_a, res_b, res_c

if __name__ == "__main__":

  t0 = time()
  # the dag instance is reusable.
  # This is recommended if you want to do the same computation multiple times
  res_a, res_b, res_c = deps_describer()
  execution_time = time() - t0
  print(f"Graph execution took {execution_time:.2f} seconds")
  assert res_a == "A"
  assert res_b == "B"
  assert res_c == "A + B = C"


```

<!-- ## Limitations:
Currently there are some limitations in the usage of tawazi that will be overcome in the future.
1. A DAG can not reuse the same function twice inside the calculation sequence (Will be resolved in the future)
2. All code inside a dag descriptor function must be either an @op decorated functions calls and arguments passed arguments. Otherwise the behavior of the DAG might be unpredicatble -->



## Name explanation
The libraries name is inspired from the arabic word تَوَازٍ which means parallel.

## Building the doc
The general documentation has no dedicated space at the moment and is being hosted offline (on your machine).
Expect future developments on this side as well. To build it, simply run `mkdocs build` and `mkdocs serve` at the root of the repository.

## Future developments
*This library is still in development. Breaking changes are expected.
