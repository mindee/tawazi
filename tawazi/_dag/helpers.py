import asyncio
import contextvars
import functools
import logging
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from copy import copy
from typing import Any, Callable, TypeVar

from typing_extensions import ParamSpec

from tawazi._dag.digraph import DiGraphEx
from tawazi._helpers import StrictDict
from tawazi.consts import Identifier, Resource, RVTypes
from tawazi.errors import TawaziTypeError
from tawazi.node import ExecNode, ReturnUXNsType, UsageExecNode
from tawazi.profile import Profile

logger = logging.getLogger(__name__)
K = TypeVar("K")
V = TypeVar("V")


def _xn_active_in_call(xn: ExecNode, results: dict[Identifier, Any]) -> bool:
    """Check if a node is active.

    Args:
        xn: the execnode
        results: dict containing the results of the execution

    Returns:
        is the node active
    """
    if xn.active is None:
        return True
    return bool(results[xn.active.id])


def copy_non_setup_xns(x_nodes: StrictDict[str, ExecNode]) -> StrictDict[str, ExecNode]:
    """Deep copy all ExecNodes except setup ExecNodes because they are shared throughout the DAG instance.

    Args:
        x_nodes: Dict[str, ExecNode] x_nodes to be deep copied

    Returns:
        Dict[str, ExecNode] copy of x_nodes
    """
    x_nodes_copy: StrictDict[str, ExecNode] = StrictDict()
    for id_, x_nd in x_nodes.items():
        # if execnode is a setup node, it shouldn't be copied
        if x_nd.setup:
            x_nodes_copy[id_] = x_nd
        else:
            # no need to deepcopy. we only need to know if self.result is NoVal or not (TODO: fix this COmment)
            x_nodes_copy[id_] = copy(x_nd)
    return x_nodes_copy


class BiDict(dict[K, V]):
    """A bidirectional dictionary that raises an error if two keys are mapped to the same value.

    >>> b = BiDict({1: 'one', 2: 'two'})
    >>> b[1]
    'one'
    >>> b.inverse['one']
    1
    >>> b[3] = 'three'
    >>> b.inverse['three']
    3
    >>> b[3] = 'threeeee'
    >>> b.inverse['threeeee']
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
    >>> BiDict({1: 2, 3: 2})
    Traceback (most recent call last):
    ...
    ValueError: Value 2 is already in the BiDict
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.inverse: dict[V, K] = {}
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


# TODO: refactor these functions by re-using the common parts
def wait_for_finished_nodes(
    return_when: str,
    graph: DiGraphEx,
    futures: BiDict[Identifier, "Future[Any]"],
    done: set["Future[Any]"],
    running: set["Future[Any]"],
    runnable_xns_ids: set[Identifier],
) -> tuple[set["Future[Any]"], set["Future[Any]"], set[Identifier]]:
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
    if len(running) == 0:
        return done, running, runnable_xns_ids
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


async def wait_for_finished_nodes_async(
    return_when: str,
    graph: DiGraphEx,
    futures: BiDict[Identifier, "asyncio.Future[Any]"],
    done: set["asyncio.Future[Any]"],
    running: set["asyncio.Future[Any]"],
    runnable_xns_ids: set[Identifier],
) -> tuple[set["asyncio.Future[Any]"], set["asyncio.Future[Any]"], set[Identifier]]:
    """Wait for the finished futures before pruning them from the graph.

    Args:
        return_when: condition for thread return
        graph: the directed graph
        futures: the futures to id map and reverse map
        done: the set of finished futures
        running: the running async-threads
        runnable_xns_ids: the exec nodes that are available to be run

    Returns:
        finisehd futures, running futures and runnable nodes
    """
    if len(running) == 0:
        return done, running, runnable_xns_ids
    done_, running = await asyncio.wait(running, return_when=return_when)
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


Param = ParamSpec("Param")
R = TypeVar("R")


def preserve_context(
    func: Callable[Param, R], *args: Param.args, **kwargs: Param.kwargs
) -> Callable[..., R]:
    """Wrap a function to keep the context of parent."""
    ctx = contextvars.copy_context()
    return functools.partial(ctx.run, func, *args, **kwargs)


async def to_thread_in_executor(
    func: Callable[..., Any], executor: ThreadPoolExecutor
) -> "asyncio.Future[Any]":
    """A modified copy of asyncio.to_thread.

    Asynchronously run function *func* in a separate thread.
    Uses the same thread pool executor for all threads.
    Doesn't handle passing the context from parent to child thread.

    Args:
        func: The function to run in the thread.
        executor: The executor to use.

    Return a coroutine that can be awaited to get the result of *func*.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, func)


################
# The scheduler!
################
def sync_execute(
    exec_nodes: StrictDict[Identifier, ExecNode],
    results: StrictDict[Identifier, Any],
    max_concurrency: int,
    graph: DiGraphEx,
) -> tuple[
    StrictDict[Identifier, ExecNode], StrictDict[Identifier, Any], StrictDict[Identifier, Profile]
]:
    """Look at the execute function for more information."""
    return asyncio.run(
        async_execute(
            exec_nodes=exec_nodes, results=results, max_concurrency=max_concurrency, graph=graph
        )
    )


async def async_execute(
    *,
    exec_nodes: StrictDict[Identifier, ExecNode],
    results: StrictDict[Identifier, Any],
    max_concurrency: int,
    graph: DiGraphEx,
) -> tuple[
    StrictDict[Identifier, ExecNode], StrictDict[Identifier, Any], StrictDict[Identifier, Profile]
]:
    """Thread safe execution of the DAG.

    (Except for the setup nodes! Please run DAG.setup() in a single thread because its results will be cached).

    Args:
        exec_nodes: dictionary identifying ExecNodes.
        results: dictionary containing results of setup and constants
        max_concurrency: maximum number of threads to be used for the execution.
        graph: the graph ids to be executed

    Returns:
        exec_nodes: dictionary with keys the name of the function and value the result after the execution
    """
    # 0.1 copy results because it will be modified here
    results = copy(results)
    profiles: StrictDict[Identifier, Profile] = StrictDict()

    # TODO: remove copy of ExecNodes when profiling and is_active are stored outside of ExecNode
    exec_nodes = copy_non_setup_xns(exec_nodes)

    # 0.2 prune the graph from the ArgExecNodes and setup ExecNodes that are already executed
    # so that they don't get executed in the ThreadPool
    graph.remove_nodes_from([id_ for id_ in graph if id_ in results])

    # 0.3 create variables related to futures
    conc_futures: BiDict[Identifier, Future[Any]] = BiDict()
    conc_done: set[Future[Any]] = set()
    conc_running: set[Future[Any]] = set()

    async_futures: BiDict[Identifier, asyncio.Future[Any]] = BiDict()
    async_done: set[asyncio.Future[Any]] = set()
    async_running: set[asyncio.Future[Any]] = set()

    def running_threads() -> int:
        return len(conc_running) + len(async_running)

    # 0.4 get the candidates root nodes that can be executed
    # runnable_nodes_ids will be empty if all root nodes are running
    runnable_xns_ids = graph.root_nodes

    # avoid having a lot of indentations
    executor = ThreadPoolExecutor(max_workers=max_concurrency)
    executor.__enter__()

    while len(graph):
        # Attempt to run **A SINGLE** root node.

        # 6. block scheduler execution if no root node can be executed.
        #    this can occur in two cases:
        #       1. if maximum thread pool concurrency is reached
        #       2. if no runnable node exists (i.e. all root nodes are being executed)
        #    in both cases: block until a node finishes
        #       => a new root node will be available
        # must wait and not submit any workers before a worker ends
        # (that might create a new more prioritized node) to be executed
        if running_threads() == max_concurrency or len(runnable_xns_ids) == 0:
            # 1st block and wait for async threads to finish.
            #  prefer giving the hand to the event loop
            logger.debug(
                "Waiting for ExecNodes threaded async {} to finish. Finished running {}",
                async_running,
                async_done,
            )
            async_done, async_running, runnable_xns_ids = await wait_for_finished_nodes_async(
                FIRST_COMPLETED, graph, async_futures, async_done, async_running, runnable_xns_ids
            )
            logger.debug(
                "Waiting for ExecNodes threaded {} to finish. Finished running {}",
                conc_running,
                conc_done,
            )
            conc_done, conc_running, runnable_xns_ids = wait_for_finished_nodes(
                FIRST_COMPLETED, graph, conc_futures, conc_done, conc_running, runnable_xns_ids
            )

        # 3. if no runnable node exist, go to step 6 (wait for a node to finish)
        #   (This **might** create a new root node)
        if len(runnable_xns_ids) == 0:
            logger.debug("No runnable Nodes available")
            continue

        # 4.1 choose the most prioritized node to run
        highest_priority_id = max(runnable_xns_ids, key=lambda id_: graph.compound_priority[id_])
        xn = exec_nodes[highest_priority_id]

        logger.debug("{} will run!", xn.id)

        # 4.2 if the current node must be run sequentially, wait for a running node to finish.
        # in that case we must prune the graph to re-check whether a new root node
        # (maybe with a higher priority) has been created => continue the loop
        # Note: This step might run a number of times in the while loop
        #       before the exec_node gets submitted
        if xn.is_sequential and running_threads() != 0:
            logger.debug(
                "{} must not run in parallel. Wait for the end of a node in {}", xn.id, conc_running
            )
            async_done, async_running, runnable_xns_ids = await wait_for_finished_nodes_async(
                FIRST_COMPLETED, graph, async_futures, async_done, async_running, runnable_xns_ids
            )
            conc_done, conc_running, runnable_xns_ids = wait_for_finished_nodes(
                FIRST_COMPLETED, graph, conc_futures, conc_done, conc_running, runnable_xns_ids
            )
            continue

        # xn will definitely be executed
        runnable_xns_ids.remove(xn.id)

        # 5.1 dynamic execution of a node
        if not _xn_active_in_call(xn, results):
            logger.debug("Prune {} from the graph", xn.id)
            results[xn.id] = None
            runnable_xns_ids |= graph.remove_root_node(xn.id)
            # if node is starting point of a subgraph, the whole subgraph should be skipped
            # by assigning None to all nodes in the subgraph
            continue

        # 5.2 submit the exec node to the executor
        if xn.resource == Resource.thread:
            exec_future_sync = executor.submit(
                preserve_context(xn.execute, results=results, profiles=profiles)
            )
            conc_running.add(exec_future_sync)
            conc_futures[xn.id] = exec_future_sync
        elif xn.resource == Resource.async_thread:
            exec_future_async = asyncio.ensure_future(
                to_thread_in_executor(
                    preserve_context(xn.execute, results=results, profiles=profiles), executor
                )
            )
            logger.debug("Submitted ExecNode {} to the ThreadPool in async mode", xn.id)
            async_running.add(exec_future_async)
            async_futures[xn.id] = exec_future_async
        else:
            # a single execution will be launched and will end.
            # it doesn't count as an additional thread that is running.
            logger.debug("Executing {} in main thread", xn.id)
            xn.execute(results=results, profiles=profiles)

            logger.debug("Remove ExecNode {} from the graph", xn.id)
            runnable_xns_ids |= graph.remove_root_node(xn.id)

        # 5.3 wait for the sequential node to finish
        # This code is executed only if this node is being executed purely by itself
        if xn.is_sequential:
            logger.debug("Wait for all Futures to finish because {} is sequential.", xn.id)
            # ALL_COMPLETED is equivalent to FIRST_COMPLETED because there is only a single future running!
            async_done, async_running, runnable_xns_ids = await wait_for_finished_nodes_async(
                ALL_COMPLETED, graph, async_futures, async_done, async_running, runnable_xns_ids
            )
            conc_done, conc_running, runnable_xns_ids = wait_for_finished_nodes(
                ALL_COMPLETED, graph, conc_futures, conc_done, conc_running, runnable_xns_ids
            )

    executor.__exit__(None, None, None)

    return exec_nodes, results, profiles


def get_return_values(return_uxns: ReturnUXNsType, results: dict[Identifier, Any]) -> RVTypes:
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
    results: StrictDict[Identifier, Any], input_uxns: list[UsageExecNode], *args: Any
) -> StrictDict[Identifier, Any]:
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

            # overriding constant ExecNodes that already have a value is allowed
            results.force_set(node_id, arg)

    return results
