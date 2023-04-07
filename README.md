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

You can find the documentation here: [Tawazi](https://mindee.github.io/tawazi/)

**Note**: The library is still at an [advanced state of development](#future-developments). Breaking changes might happen on the minor version (v0.Minor.Patch). Please pin [Tawazi](https://pypi.org/project/tawazi/) to the __Minor Version__. Your contributions are highly welcomed.

## Name explanation
The libraries name is inspired from the arabic word تَوَازٍ which means parallel.

## Building the doc
The general documentation has no dedicated space at the moment and is being hosted offline (on your machine).
Expect future developments on this side as well. To build it, simply run `mkdocs build` and `mkdocs serve` at the root of the repository.

## Future developments
__This library is still in development. Breaking changes are expected.__
