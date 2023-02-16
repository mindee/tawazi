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
* Thread Safe
* Few dependencies
* Legacy Python versions support (in the future)
* pypy support (in the future)
* Many Python implementations support (in the future)

In the context of tawazi, the computation sequence to be run in parallel is referred to as DAG and the functions that must run in parallel are called `ExecNode`s.

This library supports:
* Limiting the number of "Threads" that the DAG uses while running
* Priority Choice of each `ExecNode`
* Per `ExecNode` choice of parallelization (i.e. An `ExecNode` is allowed to run in parallel with other `ExecNode`s or not)
* setup `ExecNode`s: These nodes only run once per DAG instance
* debug `ExecNode`s: These are nodes that only run during when `RUN_DEBUG_NODES` environment variable is set
* running a subgraph of the DAG instance

**Note**: The library is still at an [advanced state of development](#future-developments). Your contributions are highly welcomed.


## Usage

### Parallelism
You can use Tawazi to make your non CPU-Bound code Faster

```python
# type: ignore
from time import sleep, time
from tawazi import xn, dag

@xn
def a():
    print("Function 'a' is running", flush=True)
    sleep(1)
    return "A"

@xn
def b():
    print("Function 'b' is running", flush=True)
    sleep(1)
    return "B"

@xn
def c(a, b):
    print("Function 'c' is running", flush=True)
    print(f"Function 'c' received {a} from 'a' & {b} from 'b'", flush=True)
    return f"{a} + {b} = C"

@dag(max_concurrency=2)
def pipeline():
  res_a = a()
  res_b = b()
  res_c = c(res_a, res_b)
  return res_c

t0 = time()
# executing the dag takes a single line of code
res = pipeline()
execution_time = time() - t0
assert execution_time < 1.5
print(f"Graph execution took {execution_time:.2f} seconds")
print(f"res = {res}")

```
As you can see, the execution time of pipeline takes less than 2 seconds, which means that some part of the code ran in parallel to the other

### pipeline

* You can pass in arguments to the pipeline and get results back using the normal function interface:

```Python
from tawazi import xn, dag
@xn
def xn1(i):
  return i+1

@xn
def xn2(i, j=1):
  return i + j + 1

@dag
def pipeline(i=0):
  res1 = xn1(i)
  res2 = xn2(res1)
  return res2

# run pipeline with default parameters
assert pipeline() == 3
# run pipeline with passed parameters
assert pipeline(1) == 4
```
You can not pass in named parameters though... (will be supported in future releases)

* You can return multiple values from a pipeline via tuples or lists. (Dict will be supported in the future)

```Python
@dag
def pipeline():
  return xn1(1), xn2(1)

assert pipeline() == (2, 3)
```

Currently you can only return a single value from an `ExecNode`, in the future multiple return values will be allowed.


* You can have setup `ExecNode`s; Theses are `ExecNode`s that will run once per DAG instance

```Python
from copy import deepcopy
@xn(setup=True)
def setop():
  global setop_counter
  setop_counter += 1
  return "Long algorithm to generate Constant Data"
@xn
def my_print(arg):
  print(arg)
@dag
def pipeline():
  cst_data = setop()
  my_print(cst_data)

setop_counter = 0
# create another instance because setup ExecNode result is cached inside the instance
pipe1 = deepcopy(pipeline)
pipe1.setup()
assert setop_counter == 1
pipe1()
assert setop_counter == 1


setop_counter = 0
pipe2 = deepcopy(pipeline)
pipe2()
assert setop_counter == 1
pipe2()
assert setop_counter == 1

```

* You can Make Debug ExecNodes that will only run if `RUN_DEBUG_NODES` env variable is set. These can be visualization ExecNodes for example... or some complicated Assertions that helps you debug problems when needed that are hostile to the production environment
```Python
@xn
def a(i):
  return i + 1
@xn(debug=True)
def print_debug(i):
  global debug_has_run
  debug_has_run = True
  print("DEBUG: ", i)
@dag
def pipe():
  i = a(0)
  print_debug(i)
  return i

debug_has_run = False
pipe()
assert debug_has_run == False

```

* Tags: you can tag an ExecNode
```Python
@xn(tag="twinkle toes")
def a():
  print("I am tough")
@xn
def b():
  print("I am normal")

@dag
def pipeline():
  a()
  b()

pipeline()
xn_a = pipeline.get_nodes_by_tag("twinkle toes")
# You can do whatever you want with this ExecNode...


# You can even tag a specific call of an ExecNode
@xn
def g(i):
  return i
@xn(tag="c_node")
def c(i):
  print(i)
@dag
def pipeline():
  b()
  hello = g("hello")
  c(hello)
  goodbye = g("goodbye")
  c(goodbye, twz_tag="byebye")

pipeline()
# multiple nodes can have the same tag!
xns_bye = pipeline.get_nodes_by_tag("byebye")
```

This will be useful if you want to run a subgraph (cf. the next paragraph). It will also be useful if you want to access result of a specific ExecNode after an Execution
* You can run a subgraph of your pipeline. During the invocation of the pipeline, you can choose your target ExecNodes (these are the leaf nodes that you want to run).
```Python
# You can use the original __qual__name of the decorated function as an Identifier
pipeline(target_nodes=["b"])
# You can use the tag of an ExecNode
pipeline(target_nodes=["c_node"])
# You can use the calling tag to distinguish the 1st call of g from the 2nd call!
pipeline(target_nodes=["byebye"])
# You can even pass in the ExecNodes to run and mix identifiers types
pipeline(target_nodes=["b", xns_bye[0]])

```

* More functionalities will be introduced in the future...


## Advanced Usage

```python

# type: ignore
from time import sleep, time
from tawazi import xn, dag

@xn
def a():
    print("Function 'a' is running", flush=True)
    sleep(1)
    return "A"

# optionally configure each ExecNode using the decorator:
# is_sequential = True to prevent ExecNode from running in parallel with other ExecNodes
# priority to choose the ExecNode in the next execution phase
@xn(is_sequential=True, priority=10)
def b():
    print("Function 'b' is running", flush=True)
    sleep(1)
    return "B"

@xn
def c(a, arg_b):
    print("Function 'c' is running", flush=True)
    print(f"Function 'c' received {a} from 'a' & {arg_b} from 'b'", flush=True)
    return f"{a} + {arg_b} = C"

# optionally customize the DAG
@dag(max_concurrency=2, behavior="strict")
def deps_describer():
  res_a = a()
  res_b = b()
  res_c = c(res_a, res_b)
  return res_a, res_b, res_c


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
