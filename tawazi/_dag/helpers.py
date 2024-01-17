from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from copy import copy
from typing import Any, Dict, List, Set, Tuple, Union

from loguru import logger

from tawazi._dag.digraph import DiGraphEx
from tawazi.consts import Identifier, Resource, RVTypes
from tawazi.errors import TawaziTypeError
from tawazi.node import ExecNode, ReturnUXNsType, UsageExecNode
from tawazi.profile import Profile


def _xn_active_in_call(
    xn: ExecNode,
    results: Dict[Identifier, Any],
    actives: Dict[Identifier, Union[Any, UsageExecNode]],
) -> bool:
    """Check if a node is active.

    Args:
        xn: the execnode
        results: dict containing the results of the execution
        actives: dict containing active data of all ExecNodes in current DAG

    Returns:
        is the node active
    """
    active = actives.get(xn.id, True)
    if isinstance(active, UsageExecNode):
        return bool(results[active.id])
    return bool(active)


def copy_non_setup_xns(x_nodes: Dict[str, ExecNode]) -> Dict[str, ExecNode]:
    """Deep copy all ExecNodes except setup ExecNodes because they are shared throughout the DAG instance.

    Args:
        x_nodes: Dict[str, ExecNode] x_nodes to be deep copied

    Returns:
        Dict[str, ExecNode] copy of x_nodes
    """
    x_nodes_copy = {}
    for id_, x_nd in x_nodes.items():
        # if execnode is a setup node, it shouldn't be copied
        if x_nd.setup:
            x_nodes_copy[id_] = x_nd
        else:
            # no need to deepcopy. we only need to know if self.result is NoVal or not (TODO: fix this COmment)
            x_nodes_copy[id_] = copy(x_nd)
    return x_nodes_copy


def get_num_running_threads(_futures: Dict[Identifier, "Future[Any]"]) -> int:
    """Get the number of running threads from a list of futures.

    Args:
        _futures: the list of futures.

    Returns:
        the number of still running threads
    """
    # use not future.done() because there is no guarantee that Thread pool will directly execute
    # the submitted thread
    return sum([not future.done() for future in _futures.values()])


def get_highest_priority_node(
    graph: DiGraphEx, runnable_xns_ids: List[str], xns_dict: Dict[Identifier, ExecNode]
) -> ExecNode:
    """Get the node with the highest priority.

    Args:
        graph: the graph with nodes ids.
        runnable_xns_ids: the nodes whitelisted for running
        xns_dict: the mapping between nodes ids and execnodes

    Returns:
        the node with the highest priority
    """
    highest_priority_node, _ = max(
        [
            (node, priority)
            for node, priority in graph.nodes(data="compound_priority")
            if node in runnable_xns_ids
        ],
        key=lambda x: x[1],
    )

    return xns_dict[highest_priority_node]


################
# The scheduler!
################
def execute(
    *,
    exec_nodes: Dict[Identifier, ExecNode],
    results: Dict[Identifier, Any],
    actives: Dict[Identifier, Union[Any, UsageExecNode]],
    max_concurrency: int,
    graph: DiGraphEx,
) -> Tuple[Dict[Identifier, ExecNode], Dict[Identifier, Any], Dict[Identifier, Profile]]:
    """Thread safe execution of the DAG.

    (Except for the setup nodes! Please run DAG.setup() in a single thread because its results will be cached).

    Args:
        exec_nodes: dictionary identifying ExecNodes.
        results: dictionary containing results of setup and constants
        actives: actives information of all ExecNodes
        max_concurrency: maximum number of threads to be used for the execution.
        behavior: the behavior to be used in case of error.
        graph: the graph ids to be executed
        modified_exec_nodes: A dictionary of the ExecNodes that have been modified by setting the input parameters of the DAG.

    Returns:
        exec_nodes: dictionary with keys the name of the function and value the result after the execution
    """
    # 0.1 copy results because it will be modified here
    results = copy(results)
    actives = copy(actives)
    profiles: Dict[Identifier, Profile] = {}

    # TODO: remove copy of ExecNodes when profiling and is_active are stored outside of ExecNode
    exec_nodes = copy_non_setup_xns(exec_nodes)

    # 0.2 prune the graph from the ArgExecNodes and setup ExecNodes that are already executed
    # so that they don't get executed in the ThreadPool
    graph.remove_nodes_from([id_ for id_ in graph if id_ in results])

    # 0.3 create variables related to futures
    futures: Dict[Identifier, "Future[Any]"] = {}
    done: Set["Future[Any]"] = set()
    running: Set["Future[Any]"] = set()

    # 0.4 get the candidates root nodes that can be executed
    # runnable_nodes_ids will be empty if all root nodes are running
    runnable_xns_ids = graph.root_nodes

    with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
        while len(graph):
            # Attempt to run **A SINGLE** root node.

            # 6. block scheduler execution if no root node can be executed.
            #    this can occur in two cases:
            #       1. if maximum concurrency is reached
            #       2. if no runnable node exists (i.e. all root nodes are being executed)
            #    in both cases: block until a node finishes
            #       => a new root node will be available
            num_running_threads = get_num_running_threads(futures)
            if num_running_threads == max_concurrency or len(runnable_xns_ids) == 0:
                # must wait and not submit any workers before a worker ends
                # (that might create a new more prioritized node) to be executed
                logger.debug(
                    "Waiting for ExecNodes %s to finish. Finished running %", running, done
                )
                done_, running = wait(running, return_when=FIRST_COMPLETED)
                done = done.union(done_)

            # 1. among the finished futures:
            #       1. checks for exceptions
            #       2. and remove them from the graph
            for id_, fut in futures.items():
                if fut.done() and id_ in graph:
                    logger.debug("Remove ExecNode % from the graph", id_)
                    # will raise the first encountered exception if there's one
                    _ = fut.result()

                    graph.remove_node(id_)

            # 2. list the root nodes that aren't being executed
            runnable_xns_ids = list(set(graph.root_nodes) - set(futures.keys()))

            # 3. if no runnable node exist, go to step 6 (wait for a node to finish)
            #   (This **might** create a new root node)
            if len(runnable_xns_ids) == 0:
                logger.debug("No runnable Nodes available")
                continue

            # 4.1 choose the most prioritized node to run
            xn = get_highest_priority_node(graph, runnable_xns_ids, exec_nodes)

            logger.info("%s will run!", xn.id)

            # 4.2 if the current node must be run sequentially, wait for a running node to finish.
            # in that case we must prune the graph to re-check whether a new root node
            # (maybe with a higher priority) has been created => continue the loop
            # Note: This step might run a number of times in the while loop
            #       before the exec_node gets submitted
            num_running_threads = get_num_running_threads(futures)
            if xn.is_sequential and num_running_threads != 0:
                logger.debug(
                    "%s must not run in parallel. Wait for the end of a node in %s", xn.id, running
                )
                done_, running = wait(running, return_when=FIRST_COMPLETED)
                # go to step 6
                continue

            # 5.1 dynamic graph pruning
            if not _xn_active_in_call(xn, results, actives):
                logger.debug("Prune %s from the graph", xn.id)
                graph.remove_recursively(xn.id)
                continue

            # 5.2 submit the exec node to the executor
            if xn.resource == Resource.thread:
                exec_future = executor.submit(xn.execute, results=results, profiles=profiles)
                running.add(exec_future)
                futures[xn.id] = exec_future
            else:
                # a single execution will be launched and will end.
                # it doesn't count as an additional thread that is running.
                logger.debug("Executing %s in main thread", xn.id)
                xn.execute(results=results, profiles=profiles)

                logger.debug("Remove ExecNode % from the graph", xn.id)
                graph.remove_node(xn.id)

            # 5.3 wait for the sequential node to finish
            # This code is executed only if this node is being executed purely by itself
            if xn.resource == Resource.thread and xn.is_sequential:
                logger.debug("Wait for all Futures to finish because %s is sequential.", xn.id)
                # ALL_COMPLETED is equivalent to FIRST_COMPLETED because there is only a single future running!
                done_, running = wait(futures.values(), return_when=ALL_COMPLETED)

    return exec_nodes, results, profiles


def get_return_values(return_uxns: ReturnUXNsType, results: Dict[Identifier, Any]) -> RVTypes:
    """Extract the return value/values from the output of the DAG's scheduler!

    Args:
        return_uxns: the return execnodes
        results: the results of the execution of the DAG's ExecNodes

    Raises:
        TawaziTypeError: if the type of the return value is not compatible with RVTypes

    Returns:
        RVTypes: the actual values extracted from xn_dict
    """
    if return_uxns is None:
        return None
    if isinstance(return_uxns, UsageExecNode):
        return return_uxns.result(results)
    if isinstance(return_uxns, (tuple, list)):
        gen = (ren_uxn.result(results) for ren_uxn in return_uxns)
        if isinstance(return_uxns, tuple):
            return tuple(gen)
        if isinstance(return_uxns, list):
            return list(gen)
    if isinstance(return_uxns, dict):
        return {key: ren_uxn.result(results) for key, ren_uxn in return_uxns.items()}

    raise TawaziTypeError("Return type for the DAG can only be a single value, Tuple or List")


def extend_results_with_args(
    results: Dict[Identifier, Any], input_uxns: List[UsageExecNode], *args: Any
) -> Dict[Identifier, Any]:
    """Extends the results of dict with the values provided by args.

    Args:
        results: results of the DAG (Constant arguments of ExecNodes and setup ExecNodes)
        input_uxns: the input execnodes
        *args (Any): arguments to be passed to the call of the DAG

    Returns:
        Dict[Identifier, ExecNode]: The modified ExecNode dict which will be executed by the DAG scheduler.

    Raises:
        TypeError: If called with an invalid number of arguments
    """
    # copy results in order to avoid modifying the original dict
    results = copy(results)
    # 2. parse the input arguments of the pipeline
    # 2.1 default valued arguments can be skipped and not provided!
    # note: if not enough arguments are provided then the code will fail
    # inside the DAG's execution through the raise_err lambda
    if args:
        # 2.2 can't provide more than enough arguments
        if len(args) > len(input_uxns):
            raise TypeError(
                f"The DAG takes a maximum of {len(input_uxns)} arguments. {len(args)} arguments provided"
            )

        # 2.3 modify ExecNodes corresponding to input ArgExecNodes
        for ind_arg, arg in enumerate(args):
            node_id = input_uxns[ind_arg].id

            results[node_id] = arg

    return results
