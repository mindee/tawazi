# Changelog

## v0.3.0 (2023-05-31)

### Improvement

* Run the ExecNode’s function with arbitrary arguments names
* pass in arguments & return values from created DAGs like normal functions
* Setup ExecNodes
* Debug ExecNodes
* Tag an ExecNode or multiple ExecNodes
* ExecNode is reusable (even inside the same DAG)
* Create a SubDag by specifying output ExecNodes of the DAG using Tag, id or the ExecNode itself
* Setup ExecNode do not accept dynamic arguments (arguments that can change between executions)
* DAG can return multiple values via dict, tuple or list
* Cache results of a specific DAG execution inside a file and reuse it later to skip running the same executions again
* An ExecNode can not be debug and setup at the same time!
* Configure ExecNodes using yaml, json or a python dict
* Profile the DAG execution (profiling of each ExecNode's execution)
* Create a SubDag by specifying ExecNodes to exclude of the DAG using Tag, id or the ExecNode itself
* pass in a thread-name prefix to the DAG
* Cache all the dependencies of some ExecNodes but not the specified ExecNodes themselves (helpful for debugging these ExecNodes)
* Introduce DAGExecution class which is an instance holding an execution of a DAG
* DAGExecution.scheduled_nodes contains the ExecNodes that will be executed in the next DAGExecution.run() call
* Support mypy typing
* Add a documentation page using mkdocs
* A single ExecNode can have multiple Tags
* + - * ** / // % divmod abs << >> < <= == > >= operations implemented for ExecNode
* Conditionally execute an ExecNode inside a DAG using `twz_active`
* helpers and_, or_, not_ are provided to do logical operations on ExecNodes
* LazyExecNode can be executed outside of DAG description according to env var TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR
* unpack the results of an ExecNode execution using `unpack_to` property
* Check `unpack_to` property against typing
* Experimental compose a subdag from a single DAG by choosing the input ExecNodes and the output ExecNodes
* ExecNode can wrap anonymous functions that have no __qualname__ attribute
* Choose where an ExecNode is executed (in main-thread or in a thread-pool)
* Document Usage of the Library

### Breaking Changes
* Improved Interface to create DAGs using `dag` decorator
* Improved Interface to create ExecNodes using `xn` decorator

### Internal Changes
* Use NoVal instead of None to express not yet calculated result in ExecNode
* Copy ExecNode instead of deep-copying it
* Run tests against Python 3.7, 3.8, 3.9, 3.10, 3.11
* Run tests against the documentation
* Test Mypy
* Use Ruff in pre-commit
* use pytest code-blocks instead of mkcodes
* CI tests building the documentation

## v0.2.0 (2022-10-17)

### New
* :green_heart: CI with tests on 6 different environments. (Coverage: **99%**)
* :white_check_mark: Tested on python 3.6, 3.7, 3.8, 3.9, 3.10.
* :white_check_mark: Tested on Windows and Linux.
* :recycle: Change the default DAG interface creation to make it ergonomic.
* :zap: choose Nodes that have more successors over Nodes that no successors.
* :sparkles: execute the DAG safely without using the scheduler.
* :sparkles: test the code in the README.
* :wrench: :technologist: Dynamic configuration of the library with custom environment variables.
* :memo: :technologist: Static general documentation with mkdocs.
* :label: Repository passes mypy (not tested on the `--strict` option which is too coercive at the moment).
* :loud_sound: Changed logging to loguru by default.
* :sparkles: Implementation of the subgraph execution feature.
* :green_heart: Dependabot integration.
* :arrow_up: Switched from requirements to poetry for dependency management.
* :art: Revamp of the repository.
* :technologist: :green_heart: Better pre-commit configuration for ease of development.

## v0.1.0 (2022-07-20)

### New
* :sparkles: first functional iteration of the DAG
