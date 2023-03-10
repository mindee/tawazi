# tawazi
<!--Python badges -->
[![Python 3.7](https://img.shields.io/badge/python-3.7%20|%203.8%20|%203.9%20|%203.10%20|%203.11-blue.svg)](https://www.python.org/)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![CodeFactor](https://www.codefactor.io/repository/github/mindee/tawazi/badge)](https://www.codefactor.io/repository/github/mindee/tawazi)
[![Downloads](https://img.shields.io/pypi/dm/tawazi)](https://pypi.org/project/tawazi/)

<!--Tawazi Badge-->
![Tawazi GIF](documentation/tawazi_GIF.gif)

## Introduction

<!-- TODO: put a link explaining what a DAG is-->

[Tawazi](https://pypi.org/project/tawazi/) facilitates **parallel** execution of functions in a [DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph) dependency structure.

This library satisfies the following:
* Stable, robust, well tested
* lightweight
* Thread Safe
* Few dependencies
* Legacy Python versions support (in the future)
* MyPy compatible
* Many Python implementations support (in the future)

In [Tawazi](https://pypi.org/project/tawazi/), a computation sequence is referred to as `DAG`. The functions invoked inside the computation sequence are referred to as `ExecNode`s.

Current features are:
* Limiting the number of "Threads" that the `DAG` uses
* setup `ExecNode`s: These nodes only run once per DAG instance
* debug `ExecNode`s: These are nodes that only run during when `RUN_DEBUG_NODES` environment variable is set
* running a subgraph of the DAG instance
* Excluding an `ExecNode` from running
* caching the results of the execution of a `DAG` for faster subsequent execution
* Priority Choice of each `ExecNode` for fine control of execution order
* Per `ExecNode` choice of parallelization (i.e. An `ExecNode` is allowed to run in parallel with other `ExecNode`s or not)
* and more!

**Note**: The library is still at an [advanced state of development](#future-developments). Breaking changes might happen on the minor version (v0.Minor.Patch). Please pin [Tawazi](https://pypi.org/project/tawazi/) to the __Minor Version__. Your contributions are highly welcomed.


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
assert execution_time < 1.5  # a() and b() are executed in parallel
print(f"Graph execution took {execution_time:.2f} seconds")
print(f"res = {res}")

```
As you can see, the execution time of pipeline takes less than 2 seconds, which means that some part of the code ran in parallel to the other

### pipeline

* You can pass in arguments to the pipeline and get returned results back like normal functions:

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
Currently you can not pass in named parameters to the `DAG` (will be supported in future releases). (This should not be confused with passing keyworded arguments to `ExecNode`s which is possible)

* You can return multiple values from a pipeline via tuples, lists or dicts.

```Python
@dag
def pipeline_tuple():
  return xn1(1), xn2(1)

assert pipeline_tuple() == (2, 3)

@dag
def pipeline_list():
  return [xn1(1), xn2(2)]

assert pipeline_list() == [2, 4]

@dag
def pipeline_dict():
  return {"foo": xn1(1), "bar": xn2(3)}

assert pipeline_dict() == {"foo": 2, "bar": 5}
```

* You can return multiple values from `ExecNode`s:

1. Either via Python `Tuple`s and `List`s but you will have to specify the unpacking number (In the future this will no longer be needed)
```Python
@xn(unpack_to=4)
def count_tuple(val):
  return (val, val + 1, val + 2, val + 3)


@xn(unpack_to=4)
def count_list(val):
  return [val, val + 1, val + 2, val + 3]


@dag
def pipeline():
  v1, v2, v3, v4 = replicate_tuple(1)
  v5, v6, v7, v8 = replicate_list(v4)
  return v1, v2, v3, v4, v5, v6, v7, v8


assert pipeline() == [1, 2, 3, 4, 4, 5, 6, 7]
```
2. Or via indexing (`Dict` or `List` etc.):
```Python
@xn
def gen_dict(val):
  return {"k1": val, "k2": "2", "nested_list": [1 ,11, 3]}

@xn
def gen_list(val):
  return [val, val + 1, val + 2, val + 3]

@xn
def incr(val):
  return val + 1

@dag
def pipeline(val):
  d = gen_dict()
  l = gen_list(d["k1"])
  inc_val = incr(l[0])
  inc_val_2 = incr(d["nested_list"][1])
  return d, l, inc_val, inc_val_2

d, l, inc_val, inc_val_2 = pipeline(123)
assert d == {"k1": val, "k2": "2", "nested_list": [1 ,2, 3]}
assert l == [123, 124, 125, 126]
assert inc_val == 124
assert inc_val_2 == 12
```
This makes the `DAG` usage as close to using the original __pipeline__ function as possible.

* You can have setup `ExecNode`s.

Setup `ExecNode`s have their results cached in the `DAG` instance. This means that they run once per `DAG` instance. These can be used to load large consts from Disk (Machine Learning Models, Large CSV files, initialization of a resource, prewarming etc.)

```Python
LARGE_DATA = "Long algorithm to generate Constant Data"
@xn(setup=True)
def setop():
  global setop_counter
  setop_counter += 1
  return LARGE_DATA
@xn
def my_print(arg):
  print(arg)
  return arg

@dag
def pipeline():
  cst_data = setop()
  large_data = my_print(cst_data)
  return large_data

setop_counter = 0
# create another instance because setup ExecNode result is cached inside the instance
assert LARGE_DATA == pipeline()
assert setop_counter == 1
assert LARGE_DATA == pipeline()
assert setop_counter == 1 # setop_counter is skipped the second time pipe1 is invoked
```
if you want to re-run the setup `ExecNode`, you have to redeclare the `DAG` or deepcopy the original `DAG` instance
```Python
@dag
def pipeline():
  cst_data = setop()
  large_data = my_print(cst_data)
  return large_data

assert LARGE_DATA == pipeline()
assert setop_counter == 2
assert LARGE_DATA == pipeline()
assert setop_counter == 2
```
You can run the setup `ExecNode`s alone:
```Python
@dag
def pipeline():
  cst_data = setop()
  large_data = my_print(cst_data)
  return large_data

pipeline.setup()
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
* You can run a subgraph of your pipeline: Make a `DAGExecution` from your `DAG` and pass in the `ExecNode`s you want to run:
```Python
# You can use the original __qual__name of the decorated function as an Identifier
pipe_exec = pipeline.executor(target_nodes=["b"])
pipe_exec()
# You can use the tag of an ExecNode
pipe_exec = pipeline.executor(target_nodes=["c_node"])
pipe_exec()
# You can use the calling tag to distinguish the 1st call of g from the 2nd call!
pipe_exec = pipeline.executor(target_nodes=["byebye"])
pipe_exec()
# You can even pass in the ExecNodes to run and mix identifiers types
pipe_exec = pipeline.executor(target_nodes=["b", xns_bye[0]])
pipe_exec()

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
2. All code inside a dag descriptor function must be either an @op decorated functions calls and arguments passed arguments. Otherwise the behavior of the DAG might be unpredictable
3. Because the main function serves only for the purpose of describing the dependencies, the code that it executes only contain the dependencies. Hence when debugging your doce, it will be impossible to view the data movement inside this function. However, you can debug code inside of a node.
-->



## Name explanation
The libraries name is inspired from the arabic word تَوَازٍ which means parallel.

## Building the doc
The general documentation has no dedicated space at the moment and is being hosted offline (on your machine).
Expect future developments on this side as well. To build it, simply run `mkdocs build` and `mkdocs serve` at the root of the repository.

## Future developments
*This library is still in development. Breaking changes are expected.
