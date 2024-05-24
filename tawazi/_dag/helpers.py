from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from copy import copy
from typing import Any, Dict, List, Set, Tuple, TypeVar, Union

from loguru import logger

from tawazi._dag.digraph import DiGraphEx
from tawazi.consts import Identifier, Resource, RVTypes
from tawazi.errors import TawaziTypeError
from tawazi.node import ExecNode, ReturnUXNsType, UsageExecNode
from tawazi.profile import Profile

K = TypeVar("K")
V = TypeVar("V")


def _xn_active_in_call(
    xn: ExecNode,
    results: Dict[Identifier, Any],
    active_nodes: Dict[Identifier, Union[Any, UsageExecNode]],
) -> bool:
    """Check if a node is active.

    Args:
        xn: the execnode
        results: dict containing the results of the execution
        active_nodes: dict containing active data of all ExecNodes in current DAG

    Returns:
        is the node active
    """
    active = active_nodes.get(xn.id, True)
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


def get_highest_priority_node(
    graph: DiGraphEx, runnable_xns_ids: Set[str], xns_dict: Dict[Identifier, ExecNode]
) -> ExecNode:
    """Get the node with the highest priority.

    Args:
        graph: the graph with nodes ids.
        runnable_xns_ids: the nodes whitelisted for running
        xns_dict: the mapping between nodes ids and execnodes

    Returns:
        the node with the highest priority
    """
    list_runnable_xns_ids = list(runnable_xns_ids)
    max_priority = 0
    xn = xns_dict[list_runnable_xns_ids[0]]
    nodes = graph.nodes
    for runnable_node_id in list_runnable_xns_ids:
        priority = nodes[runnable_node_id]["compound_priority"]
        if max_priority < priority:
            max_priority = priority
            xn = xns_dict[runnable_node_id]
    return xn

    # highest_priority_node, _ = max(
    #     [
    #         (node, priority)
    #         for node, priority in graph.nodes(data="compound_priority")
    #         if node in runnable_xns_ids
    #     ],
    #     key=lambda x: x[1],
    # )

    # return xns_dict[highest_priority_node]


class BiDict(Dict[K, V]):
    """A bidirectional dictionary that raises an error if two keys are mapped to the same value.

    >>> b = BiDict({1: 'one', 2: 'two'})
    >>> b[1]
    'one'
    >>> b.inverse['one']
    1
    >>> b[3] = 'three'
    >>> b.inverse['three']
    3
    >>> b[4] = 'one'
    Traceback (most recent call last):
    ...
    ValueError: Value one is already in the BiDict
    >>> del b[1]
    >>> assert 1 not in b
    >>> b[4] = 'one'
    >>> b.inverse['one']
    4
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.inverse: Dict[V, K] = {}
        for key, value in self.items():
            if value in self.inverse:
                raise ValueError(f"Value {value} is already in the BiDict")
            self.inverse[value] = key

    def __setitem__(self, key: K, value: V) -> None:
        if key in self:
            del self.inverse[self[key]]
        # check for errors before setting the value
        if value in self.inverse:
            raise ValueError(f"Value {value} is already in the BiDict")
        super().__setitem__(key, value)
        self.inverse[value] = key

    def __delitem__(self, key: K) -> None:
        del self.inverse[self[key]]
        super().__delitem__(key)


def wait_for_finished_nodes(
    return_when: str,
    graph: DiGraphEx,
    futures: BiDict[Identifier, "Future[Any]"],
    done: Set["Future[Any]"],
    running: Set["Future[Any]"],
    runnable_xns_ids: Set[Identifier],
) -> Tuple[Set["Future[Any]"], Set["Future[Any]"], Set[Identifier]]:
    """Wait for the finished futures before pruning them from the graph.

    Args:
        return_when: condition for thread return
        graph: the directed graph
        futures: the futures to id map and reverse map
        done: the set of finished futures
        running: the running threads
        runnable_xns_ids: the exec nodes that are available to be run

    Returns:
        finisehd futures, running futures and runnable nodes
    """
    done_, running = wait(running, return_when=return_when)
    done = done.union(done_)

    # 1. among the finished futures:
    #   1. checks for exceptions
    #   2. and remove them from the graph
    for done_future in done_:
        future_id = futures.inverse[done_future]
        _ = futures[future_id].result()  # raise exception by calling the future
        logger.debug("Remove ExecNode {} from the graph", future_id)
        runnable_xns_ids |= graph.remove_root_node(future_id)

    return done, running, runnable_xns_ids


################
# The scheduler!
################
def execute(
    *,
    exec_nodes: Dict[Identifier, ExecNode],
    results: Dict[Identifier, Any],
    active_nodes: Dict[Identifier, Union[Any, UsageExecNode]],
    max_concurrency: int,
    graph: DiGraphEx,
) -> Tuple[Dict[Identifier, ExecNode], Dict[Identifier, Any], Dict[Identifier, Profile]]:
    """Thread safe execution of the DAG.

    (Except for the setup nodes! Please run DAG.setup() in a single thread because its results will be cached).

    Args:
        exec_nodes: dictionary identifying ExecNodes.
        results: dictionary containing results of setup and constants
        active_nodes: actives information of all ExecNodes
        max_concurrency: maximum number of threads to be used for the execution.
        graph: the graph ids to be executed

    Returns:
        exec_nodes: dictionary with keys the name of the function and value the result after the execution
    """
    # 0.1 copy results because it will be modified here
    results = copy(results)
    active_nodes = copy(active_nodes)
    profiles: Dict[Identifier, Profile] = {}

    # TODO: remove copy of ExecNodes when profiling and is_active are stored outside of ExecNode
    exec_nodes = copy_non_setup_xns(exec_nodes)

    # 0.2 prune the graph from the ArgExecNodes and setup ExecNodes that are already executed
    # so that they don't get executed in the ThreadPool
    graph.remove_nodes_from([id_ for id_ in graph if id_ in results])

    # 0.3 create variables related to futures
    futures: BiDict[Identifier, "Future[Any]"] = BiDict()
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
            if len(running) == max_concurrency or len(runnable_xns_ids) == 0:
                # must wait and not submit any workers before a worker ends
                # (that might create a new more prioritized node) to be executed
                logger.debug(
                    "Waiting for ExecNodes {} to finish. Finished running {}", running, done
                )
                done, running, runnable_xns_ids = wait_for_finished_nodes(
                    FIRST_COMPLETED, graph, futures, done, running, runnable_xns_ids
                )

            # 3. if no runnable node exist, go to step 6 (wait for a node to finish)
            #   (This **might** create a new root node)
            if len(runnable_xns_ids) == 0:
                logger.debug("No runnable Nodes available")
                continue

            # 4.1 choose the most prioritized node to run
            xn = get_highest_priority_node(graph, runnable_xns_ids, exec_nodes)

            logger.info("{} will run!", xn.id)

            # 4.2 if the current node must be run sequentially, wait for a running node to finish.
            # in that case we must prune the graph to re-check whether a new root node
            # (maybe with a higher priority) has been created => continue the loop
            # Note: This step might run a number of times in the while loop
            #       before the exec_node gets submitted
            if xn.is_sequential and len(running) != 0:
                logger.debug(
                    "{} must not run in parallel. Wait for the end of a node in {}", xn.id, running
                )
                done, running, runnable_xns_ids = wait_for_finished_nodes(
                    FIRST_COMPLETED, graph, futures, done, running, runnable_xns_ids
                )
                continue

            # xn will definitely be executed
            runnable_xns_ids.remove(xn.id)

            # 5.1 dynamic graph pruning
            if not _xn_active_in_call(xn, results, active_nodes):
                logger.debug("Prune {} from the graph", xn.id)
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
                logger.debug("Executing {} in main thread", xn.id)
                xn.execute(results=results, profiles=profiles)

                logger.debug("Remove ExecNode {} from the graph", xn.id)
                runnable_xns_ids |= graph.remove_root_node(xn.id)

            # 5.3 wait for the sequential node to finish
            # This code is executed only if this node is being executed purely by itself
            if xn.resource == Resource.thread and xn.is_sequential:
                logger.debug("Wait for all Futures to finish because {} is sequential.", xn.id)
                # ALL_COMPLETED is equivalent to FIRST_COMPLETED because there is only a single future running!
                done, running, runnable_xns_ids = wait_for_finished_nodes(
                    ALL_COMPLETED, graph, futures, done, running, runnable_xns_ids
                )

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
