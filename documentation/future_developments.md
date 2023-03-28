
### Soon to be released
A couple of features will be released soon:

* handle problems when calling `ExecNodes` wrongly.
  * (for example when using *args as parameters but only **kwargs are provided).
  * Calling `ExecNodes` must be similar to calling the original function (must imitate the same signature otherwise raise the correct exception).
* support mixing ExecNodes and non `ExecNodes` functions.
* test the case where `ExecNodes` are stored in a list and then passed via * operator.
* an example of running a set of calculations with the `DAG` and without the `DAG`.
  * we can show that using tawazi is transparent and one just has to remove the decorators. Then everything is back to normal.
* decide whether to identify the `ExecNode` by a Hashable ID or by its own Python ID. This is breaking change and must change to 0.2.1.
* support constants by resolving the error in the tests.
* Remove the global object and maybe replace it with an attribute to the creating function.
* improve the graph dependency rendering on the console (using graphviz).
* automatically generate release on new tag https://docs.github.com/en/repositories/releasing-projects-on-github/automatically-generated-release-notes#configuring-automatically-generated-release-notes
* use opnssf service to evaluate code best practices https://bestpractices.coreinfrastructure.org/fr/projects/1486

### Features to be discussed
* support multiprocessing.
* simulation of the execution using a `DAG` stored ledger.
* Disallow execution in parallel of some threads in parallel with some other threads.
  * maybe by making a group of threads that are CPU bound and a group of threads that are IO bound ?
* Remove dependency on networkx to make `tawazi` a standalone package.
* save the results of the calculation in pickled format in case an error is encountered ? or just at the end of the run
  * re-run the same calculations of the graph but take the input from the presaved pickle files instead
* put documentation about different cases where it is advantageous to use it
  * in methods not only in functions
  * in a gunicorn application
  * for getting information from multiple resources
* pretty-print the graph deps on the console:
<!--
# a code that can generate a graph from a list of deps by maintaining a spacing of 1 between
the brothers
A------
|\ \   \
B C D   E
| | |\  |
E F G H I
-->
* remove the argument_name behavior using a more intelligent way
  * or instead of doing this, you can make this optional and then prefer to infer the argument_name more intelligently using the place in the arguments where the ExecNode was passed!
