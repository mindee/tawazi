
### Soon to be released
A couple of features will be released soon:

* handle problems when calling `ExecNodes` wrongly.
  * (for example when using *args as parameters but only **kwargs are provided).
  * Calling `ExecNodes` must be similar to calling the original function (must imitate the same signature otherwise raise the correct exception).
* improve the graph dependency rendering on the console (using graphviz).
* automatically generate release on new tag https://docs.github.com/en/repositories/releasing-projects-on-github/automatically-generated-release-notes#configuring-automatically-generated-release-notes
* use opnssf service to evaluate code best practices https://bestpractices.coreinfrastructure.org/fr/projects/1486

### Features to be discussed
* support multiprocessing.
* simulation of the execution using a `DAG` stored ledger.
* Disallow execution in parallel of some threads in parallel with some other threads.
  * maybe by making a group of threads that are CPU bound and a group of threads that are IO bound ?
* save the results of the calculation in pickled format in case an error is encountered ? or just at the end of the run
  * re-run the same calculations of the graph but take the input from the presaved pickle files instead
* put documentation about different cases where it is advantageous to use it
  * in methods not only in functions
  * in a gunicorn application
