from concurrent.futures import (
    ALL_COMPLETED,
    FIRST_COMPLETED,
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from copy import copy
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar, cast

from loguru import logger
from typing_extensions import ParamSpec

from tawazi._dag.digraph import DiGraphEx
from tawazi.consts import Identifier, Resource
from tawazi.errors import ErrorStrategy
from tawazi.node.node import ExecNode


def _xn_active_in_call(xn: ExecNode, xns_dict: Dict[Identifier, ExecNode]) -> bool:
    if isinstance(xn.active, bool):
        return xn.active
    return bool(xns_dict[xn.active.id].result)


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


################
# The scheduler!
################

_P = ParamSpec("_P")
_T = TypeVar("_T")
THREAD = "thread"
PROCESS = "process"


class FutureEx(Future):  # type: ignore[type-arg]
    def __init__(self) -> None:
        super().__init__()
        self.executor_type = THREAD


class ThreadPoolExecutorEx(ThreadPoolExecutor):
    def submit(self, fn: Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs) -> FutureEx:
        future = super().submit(fn, *args, **kwargs)
        future = cast(FutureEx, future)
        future.executor_type = THREAD
        return future


class ProcessPoolExecutorEx(ProcessPoolExecutor):
    def submit(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> FutureEx:
        future = super().submit(fn, *args, **kwargs)
        future = cast(FutureEx, future)
        future.executor_type = PROCESS
        return future


def wait_ex(
    fs: Set[FutureEx], timeout: Optional[float] = None, return_when: str = ALL_COMPLETED
) -> Tuple[Set[FutureEx], Set[FutureEx]]:
    done, running = wait(fs=fs, timeout=timeout, return_when=return_when)

    return cast(Set[FutureEx], done), cast(Set[FutureEx], running)


def get_num_running_threads(_futures: Dict[Identifier, FutureEx]) -> int:
    # use not future.done() because there is no guarantee that Thread pool will directly execute
    # the submitted thread
    threads_futures = filter(
        lambda f: f.executor_type == THREAD and not f.done(), _futures.values()
    )
    return len(list(threads_futures))


def get_num_running_processes(_futures: Dict[Identifier, FutureEx]) -> int:
    # use not future.done() because there is no guarantee that Thread pool will directly execute
    # the submitted thread
    processes_futures = filter(
        lambda f: f.executor_type == PROCESS and not f.done(), _futures.values()
    )
    return len(list(processes_futures))


def get_highest_priority_nodes(nodes: List[ExecNode]) -> List[ExecNode]:
    highest_priority = max(node.priority for node in nodes)
    return [node for node in nodes if node.priority == highest_priority]


def execute(
    *,
    node_dict: Dict[Identifier, ExecNode],
    max_threads_concurrency: int,
    max_processes_concurrency: int,
    behavior: ErrorStrategy,
    graph: DiGraphEx,
    modified_node_dict: Optional[Dict[str, ExecNode]] = None,
    call_id: str = "",
) -> Dict[Identifier, Any]:
    """Thread safe execution of the DAG.

    (Except for the setup nodes! Please run DAG.setup() in a single thread because its results will be cached).

    Args:
        node_dict: dictionary identifying ExecNodes.
        max_threads_concurrency: maximum number of threads to be used for the execution.
        max_processes_concurrency: maximum number of processes to be used for the execution.
        behavior: the behavior to be used in case of error.
        graph: the graph ids to be executed
        modified_node_dict: A dictionary of the ExecNodes that have been modified by setting the input parameters of the DAG.
        call_id (str): A unique identifier for the execution.
            It can be used to distinguish the id of the call inside the thread.
            It might be useful to debug and to exchange information between the main thread and the sub-threads (per-node threads)

    Returns:
        node_dict: dictionary with keys the name of the function and value the result after the execution
    """
    # 0.1 deepcopy the node_dict in order to modify the results inside every node and make the dag reusable
    #     modified_node_dict are used to modify the values inside the ExecNode corresponding
    #     to the input arguments provided to the whole DAG (ArgExecNode)
    xns_dict = copy_non_setup_xns(node_dict) if modified_node_dict is None else modified_node_dict

    # 0.2 prune the graph from the ArgExecNodes so that they don't get executed in the ThreadPool
    precomputed_xns_ids = [id_ for id_ in graph if xns_dict[id_].executed]
    for id_ in precomputed_xns_ids:
        graph.remove_node(id_)

    # 0.3 create variables related to futures
    futures: Dict[Identifier, FutureEx] = {}
    done: Set[FutureEx] = set()
    running: Set[FutureEx] = set()

    # 0.4 get the candidates root nodes that can be executed
    # runnable_nodes_ids will be empty if all root nodes are running
    runnable_xns_ids = graph.root_nodes()

    with ProcessPoolExecutorEx(max_workers=max_processes_concurrency) as pr_executor:
        with ThreadPoolExecutorEx(
            max_workers=max_threads_concurrency, thread_name_prefix=call_id
        ) as th_executor:
            _execute(
                max_threads_concurrency=max_threads_concurrency,
                max_processes_concurrency=max_processes_concurrency,
                behavior=behavior,
                graph=graph,
                xns_dict=xns_dict,
                futures=futures,
                done=done,
                running=running,
                runnable_xns_ids=runnable_xns_ids,
                th_executor=th_executor,
                pr_executor=pr_executor,
            )

    return xns_dict


def _execute(
    *,
    max_threads_concurrency: int,
    max_processes_concurrency: int,
    behavior: ErrorStrategy,
    graph: DiGraphEx,
    xns_dict: Dict[Identifier, ExecNode],
    futures: Dict[Identifier, FutureEx],
    done: Set[FutureEx],
    running: Set[FutureEx],
    runnable_xns_ids: List[Identifier],
    th_executor: ThreadPoolExecutorEx,
    pr_executor: ProcessPoolExecutorEx,
) -> None:
    while len(graph):
        # Attempt to run **A SINGLE** root node.

        # 6. block scheduler execution if no root node can be executed.
        #    this can occur in two cases:
        #       1. if maximum threads concurrency is reached
        #       2. if maximum processes concurrency is reached
        #       3. if no runnable node exists (i.e. all root nodes are being executed)
        #    in all cases: block until a node finishes
        #       => a new root node will be available
        num_running_processes = get_num_running_processes(futures)
        num_running_threads = get_num_running_threads(futures)
        num_runnable_nodes_ids = len(runnable_xns_ids)

        if (
            num_running_threads == max_threads_concurrency
            or num_running_processes == max_processes_concurrency
            or num_runnable_nodes_ids == 0
        ):
            # must wait and not submit any workers before a worker ends
            # (that might create a new more prioritized node) to be executed
            logger.debug(f"Waiting for ExecNodes {running} to finish. Finished running {done}")
            done_, running = wait_ex(running, return_when=FIRST_COMPLETED)
            done = done.union(done_)

        # 1. among the finished futures:
        #  1. checks for exceptions
        #  2. and remove them from the graph
        for id_, fut in futures.items():
            if fut.done() and id_ in graph:
                logger.debug(f"Remove ExecNode {id_} from the graph")
                res = handle_future_exception(behavior, graph, fut, id_)
                graph.remove_node(id_)

                # Future launched in Process, xns_dict is not updated
                if fut.executor_type == PROCESS:
                    xns_dict[id_].result = res

        # 2. list the root nodes that aren't being executed
        runnable_xns_ids = list(set(graph.root_nodes()) - set(futures.keys()))

        # 3. if no runnable node exist, go to step 6 (wait for a node to finish)
        #   (This **might** create a new root node)
        if len(runnable_xns_ids) == 0:
            logger.debug("No runnable Nodes available")
            continue

        # 4. choose a node to run
        # 4.1 get the most prioritized node to run
        # 4.1.1 get all the nodes that have the highest priority
        runnable_xns = [xns_dict[node_id] for node_id in runnable_xns_ids]
        highest_priority_xns = get_highest_priority_nodes(runnable_xns)

        # 4.1.2 get the node with the highest compound priority
        # (randomly selected if multiple are suggested)
        highest_priority_xns.sort(key=lambda node: node.compound_priority)
        xn = highest_priority_xns[-1]

        logger.info(f"{xn.id} will run!")

        # 4.2 if the current node must be run sequentially, wait for a running node to finish.
        # in that case we must prune the graph to re-check whether a new root node
        # (maybe with a higher priority) has been created => continue the loop
        # Note: This step might run a number of times in the while loop
        #       before the exec_node gets submitted
        num_running_threads = get_num_running_threads(futures)
        num_running_processes = get_num_running_processes(futures)
        if xn.is_sequential and (num_running_threads != 0 or num_running_processes != 0):
            logger.debug(
                f"{xn.id} must not run in parallel." f"Wait for the end of a node in {running}"
            )
            done_, running = wait_ex(running, return_when=FIRST_COMPLETED)
            # go to step 6
            continue

        # 5.1 dynamic graph pruning
        if not _xn_active_in_call(xn, xns_dict):
            logger.debug(f"Prune {xn.id} from the graph")
            graph.remove_recursively(xn.id)
            continue

        # 5.2 submit the exec node to the executor
        if xn.resource == Resource.thread:
            exec_future = th_executor.submit(xn._execute, node_dict=xns_dict)
            running.add(exec_future)
            futures[xn.id] = exec_future
        elif xn.resource == Resource.process:
            exec_future = pr_executor.submit(xn._execute, node_dict=xns_dict)
            running.add(exec_future)
            futures[xn.id] = exec_future
        else:
            # a single execution will be launched and will end.
            # it doesn't count as an additional thread that is running.
            logger.debug(f"Executing {xn.id} in main thread")
            try:
                xn._execute(node_dict=xns_dict)
            except Exception as e:
                if behavior == ErrorStrategy.strict:
                    raise e from e
                    # else:
                handle_exception(behavior, graph, xn.id, e)

            logger.debug(f"Remove ExecNode {xn.id} from the graph")
            graph.remove_node(xn.id)

        # 5.3 wait for the sequential node to finish
        # This code is executed only if this node is being executed purely by itself
        if xn.resource != Resource.main_thread and xn.is_sequential:
            logger.debug(f"Wait for all Futures to finish because {xn.id} is sequential.")
            # ALL_COMPLETED is equivalent to FIRST_COMPLETED because there is only a single future running!
            done_, running = wait_ex(set(futures.values()), return_when=ALL_COMPLETED)


def handle_future_exception(
    behavior: ErrorStrategy, graph: DiGraphEx, fut: FutureEx, id_: Identifier
) -> Any:
    """Checks if futures have produced exceptions, and handles them according to the specified behavior.

    Args:
        behavior: the behavior to adopt in case of exception
        graph: the graph
        fut: the thread future
        id_: the Identifier of the current ExecNode

    Raises:
        NotImplementedError: if self.behavior is not known
    """
    if behavior == ErrorStrategy.strict:
        # will raise the first encountered exception if there's one
        # no simpler way to check for exception, and not supported by flake8
        return fut.result()  # noqa: F841

    try:
        return fut.result()  # noqa: F841

    except Exception as e:
        handle_exception(behavior, graph, id_, e)
        return None


def handle_exception(
    behavior: ErrorStrategy, graph: DiGraphEx, id_: Identifier, e: Exception
) -> None:
    logger.exception(f"The feature {id_} encountered the following error:")

    if behavior == ErrorStrategy.permissive:
        logger.warning("Ignoring exception as the behavior is set to permissive")

    elif behavior == ErrorStrategy.all_children:
        # remove all its children.
        # Skip removing root Node.
        # It will be removed by default afterwards outside handle_exception
        graph.remove_recursively(id_, remove_root_node=False)

    else:
        raise NotImplementedError(f"Unknown behavior name: {behavior}") from e
