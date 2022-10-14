# Future developments
*This library is still in development. Breaking changes are expected.*

## Soon to be released
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

## Features to be discussed
* support multiprocessing.
* simulation of the execution using a `DAG` stored ledger.
* Disallow execution in parallel of some threads in parallel with some other threads.
  * maybe by making a group of threads that are CPU bound and a group of threads that are IO bound ?
* Remove dependency on networkx to make `tawazi` a standalone package.
