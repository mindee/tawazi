import json
import pickle
import time
from collections import defaultdict
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from copy import copy, deepcopy
from itertools import chain
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import networkx as nx
import yaml
from loguru import logger
from networkx import find_cycle
from networkx.exception import NetworkXNoCycle, NetworkXUnfeasible
from pydantic import BaseModel, Extra, validator

from tawazi.config import Cfg
from tawazi.consts import ReturnIDsType, Tag
from tawazi.helpers import UniqueKeyLoader, filter_NoVal

from .consts import IdentityHash
from .digraph import DiGraphEx
from .errors import ErrorStrategy, TawaziTypeError, TawaziUsageError
from .node import Alias, ArgExecNode, ExecNode, LazyExecNode


class DAG(
    BaseModel,
    validate_assignment=True,
    arbitrary_types_allowed=True,
    extra=Extra.allow,
    smart_union=True,
):
    """
    Data Structure containing ExecNodes with interdependencies.
    The ExecNodes can be executed in parallel with the following restrictions:
        * Limited number of threads.
        * Parallelization constraint of each ExecNode (is_sequential attribute)
    """

    # ExecNodes can be shared between Graphs, their call signatures might also be different
    # NOTE: maybe this should be transformed into a property because there is a deepcopy for node_dict...
    #  this means that there are different ExecNodes that are hanging around in the same instance of the DAG
    exec_nodes: List[ExecNode]
    input_ids: List[IdentityHash] = []
    max_concurrency: int = 1
    behavior: ErrorStrategy = ErrorStrategy.strict
    graph_ids: DiGraphEx = DiGraphEx()

    node_dict: Dict[IdentityHash, ExecNode] = {}
    tagged_nodes: Dict[Tag, List[ExecNode]] = {}
    node_dict_by_name: Dict[str, ExecNode] = {}

    return_ids: ReturnIDsType = None

    # a sequence of execution to be applied in a for loop
    exec_node_sequence: List[ExecNode] = []

    bckrd_deps: Dict[IdentityHash, List[IdentityHash]] = {}
    frwrd_deps: Dict[IdentityHash, List[IdentityHash]] = {}

    @validator("graph_ids", pre=True, always=True)
    def build_graph(cls, digraph: DiGraphEx, values: Dict[str, Any]) -> DiGraphEx:
        # 1 add nodes
        for xn in values["exec_nodes"]:
            digraph.add_node(xn.id)

        # 2 add edges
        for xn in values["exec_nodes"]:
            edges = [(dep.id, xn.id) for dep in xn.dependencies]
            digraph.add_edges_from(edges)

        # 3. check for circular dependencies
        try:
            cycle = find_cycle(digraph)
        except NetworkXNoCycle:
            cycle = None

        if cycle:
            raise NetworkXUnfeasible(f"the graph contains at least a circular dependency: {cycle}")

        return digraph

    @validator("node_dict", pre=True, always=True)
    def fill_nodes(
        cls, value: Dict[IdentityHash, ExecNode], values: Dict[str, Any]
    ) -> Dict[IdentityHash, ExecNode]:
        return {exec_node.id: exec_node for exec_node in values["exec_nodes"]}

    @validator("bckrd_deps", pre=True, always=True)
    def build_backward_deps(
        cls, value: Dict[IdentityHash, List[IdentityHash]], values: Dict[str, Any]
    ) -> Dict[IdentityHash, List[IdentityHash]]:
        bwd_deps = {
            xn.id: list(values["graph_ids"].predecessors(xn.id)) for xn in values["exec_nodes"]
        }
        cls._assign_compound_priority(
            graph_ids=values["graph_ids"], node_dict=values["node_dict"], bckrd_deps=bwd_deps
        )

        return bwd_deps

    @validator("frwrd_deps", pre=True, always=True)
    def build_forward_deps(
        cls, value: Dict[IdentityHash, List[IdentityHash]], values: Dict[str, Any]
    ) -> Dict[IdentityHash, List[IdentityHash]]:
        return {xn.id: list(values["graph_ids"].successors(xn.id)) for xn in values["exec_nodes"]}

    @validator("tagged_nodes", pre=True, always=True)
    def compute_tags(
        cls, value: Dict[Tag, List[ExecNode]], values: Dict[str, Any]
    ) -> Dict[Tag, List[ExecNode]]:
        # Compute all the tags in the DAG to reduce overhead during computation
        tags_dict = defaultdict(list)
        for xn in values["exec_nodes"]:
            if xn.tag:
                tags_dict[xn.tag].append(xn)

        return tags_dict

    @validator("node_dict_by_name", pre=True, always=True)
    def extract_names(
        cls, value: Dict[str, ExecNode], values: Dict[str, Any]
    ) -> Dict[str, ExecNode]:
        return {exec_node._name: exec_node for exec_node in values["exec_nodes"]}

    @validator("exec_node_sequence", pre=True, always=True)
    def build_sequence_from_topological_order(
        cls, value: List[ExecNode], values: Dict[str, Any]
    ) -> List[ExecNode]:
        topological_order = values["graph_ids"].topologically_sorted

        return [values["node_dict"][xn_id] for xn_id in topological_order]

    @validator("input_ids")
    def check_input_ids(
        cls, value: List[IdentityHash], values: Dict[str, Any]
    ) -> List[IdentityHash]:
        for xn in values["exec_nodes"]:
            if xn.setup and any(dep.id in value for dep in xn.dependencies):
                raise TawaziUsageError(
                    f"The ExecNode {xn} takes as parameters one of the DAG's input parameter"
                )
        return value

    @validator("return_ids", pre=True)
    def format_return_ids(cls, returned_exec_nodes: List[ExecNode]) -> ReturnIDsType:
        err_string = (
            "Return type of the pipeline must be either a Single Xnode,"
            " Tuple of Xnodes, List of Xnodes, dict of Xnodes or None"
        )

        # No value returned by the execution
        if returned_exec_nodes is None:
            return None
        # a single value is returned
        elif isinstance(returned_exec_nodes, ExecNode):
            return returned_exec_nodes.id
        # multiple values returned
        elif isinstance(returned_exec_nodes, tuple):
            return tuple(rxn.id for rxn in returned_exec_nodes)
        elif isinstance(returned_exec_nodes, list):
            return [rxn.id for rxn in returned_exec_nodes]
        elif isinstance(returned_exec_nodes, dict):
            if set([type(v) for v in returned_exec_nodes.values()]).issubset(
                {ArgExecNode, LazyExecNode}
            ):
                return {k: rxn.id for k, rxn in returned_exec_nodes.items()}
            else:
                raise TawaziTypeError("return dict should only contain ExecNodes")
        else:
            raise TawaziTypeError(
                f"{err_string}. Type of the provided return: {type(returned_exec_nodes)}"
            )

    # getters
    def get_nodes_by_tag(self, tag: Any) -> List[ExecNode]:
        nodes = [ex_n for ex_n in self.exec_nodes if ex_n.tag == tag]
        return nodes

    def get_node_by_id(self, id_: IdentityHash) -> ExecNode:
        # TODO: ? catch the keyError and
        #   help the user know the id of the ExecNode by pointing to documentation!?
        return self.node_dict[id_]

    # TODO: get node by usage (the order of call of an ExecNode)

    @classmethod
    def _assign_compound_priority(
        cls,
        graph_ids: DiGraphEx,
        node_dict: Dict[IdentityHash, ExecNode],
        bckrd_deps: Dict[IdentityHash, List[IdentityHash]],
    ) -> None:
        """Assigns a compound priority to all nodes in the graph.

        The compound priority is the sum of the priorities of all children recursively.

        Args:
            graph_ids: the directed acyclic graph
            node_dict: the mapping between the ids and the exec nodes
            bckrd_deps: the backwards dependencies of the exec nodes
        """
        # 1. deepcopy graph_ids because it will be modified (pruned)
        graph_ids = deepcopy(graph_ids)
        leaf_ids = graph_ids.leaf_nodes

        # 2. assign the compound priority for all the remaining nodes in the graph:
        # Priority assignment happens by epochs:
        # 2.1. during every epoch, we assign the compound priority for the parents of the current leaf nodes
        # 2.2. at the end of every epoch, we trim the graph from its leaf nodes;
        #       hence the previous parents become the new leaf nodes
        while len(graph_ids) > 0:
            # Epoch level
            for leaf_id in leaf_ids:
                leaf_node = node_dict[leaf_id]

                for parent_id in bckrd_deps[leaf_id]:
                    # increment the compound_priority of the parent node by the leaf priority
                    parent_node = node_dict[parent_id]
                    parent_node.compound_priority += leaf_node.compound_priority

                # trim the graph from its leaf nodes
                graph_ids.remove_node(leaf_id)

            # assign the new leaf nodes
            leaf_ids = graph_ids.leaf_nodes

    def draw(self, k: float = 0.8, display: bool = True, t: int = 3) -> None:
        """Draws the Networkx directed graph.

        Args:
            k (float): parameter for the layout of the graph, the higher, the further the nodes apart. Defaults to 0.8.
            display (bool): display the layout created. Defaults to True.
            t (int): time to display in seconds. Defaults to 3.
        """
        import matplotlib.pyplot as plt

        # TODO: use graphviz instead! it is much more elegant

        pos = nx.spring_layout(self.graph_ids, seed=42069, k=k, iterations=20)
        nx.draw(self.graph_ids, pos, with_labels=True)
        if display:
            plt.ion()
            plt.show()
            time.sleep(t)
            plt.close()

    @classmethod
    def _copy_non_setup_xns(cls, x_nodes: Dict[str, ExecNode]) -> Dict[str, ExecNode]:
        """
        Deep copy all ExecNodes except setup ExecNodes because they are shared throughout the DAG instance

        Args:
            x_nodes: Dict[str, ExecNode] x_nodes to be deep copied

        Returns:
            Dict[str, ExecNode] copy of x_nodes
        """
        # TODO: separate setup xnodes and non setup xndoes.
        #  maybe use copy instead of deepcopy for the non setup xnodes!? I think this is a bad idea it won't work
        x_nodes_copy = {}
        for id_, x_nd in x_nodes.items():
            # if execnode is a setup node, it shouldn't be copied
            if x_nd.setup:
                x_nodes_copy[id_] = x_nd
            else:
                # no need to deepcopy. we only need to know if self.result is NoVal or not (TODO: fix this COmment)
                x_nodes_copy[id_] = copy(x_nd)
        return x_nodes_copy

    def _execute(
        self,
        graph: DiGraphEx,
        modified_node_dict: Optional[Dict[str, ExecNode]] = None,
        call_id: str = "",
    ) -> Dict[IdentityHash, Any]:
        """
        Thread safe execution of the DAG...
         (Except for the setup nodes! Please run DAG.setup() in a single thread because its results will be cached).

        Args:
            graph: the graph ids to be executed
            modified_node_dict: A dictionary of the ExecNodes that have been modified by setting the input parameters of the DAG.
            call_id (str): A unique identifier for the execution.
                It can be used to distinguish the id of the call inside the thread.
                It might be useful to debug and to exchange information between the main thread and the sub-threads (per-node threads)

        Returns:
            node_dict: dictionary with keys the name of the function and value the result after the execution
        """
        # 0.1 create a subgraph of the graph if necessary

        # 0.2 deepcopy the node_dict in order to modify the results inside every node and make the dag reusable
        #     modified_node_dict are used to modify the values inside the ExecNode corresponding
        #     to the input arguments provided to the whole DAG (ArgExecNode)
        xns_dict = modified_node_dict or DAG._copy_non_setup_xns(self.node_dict)

        # 0.3 prune the graph from the ArgExecNodes so that they don't get executed in the ThreadPool
        precomputed_xns_ids = [id_ for id_ in graph if xns_dict[id_].executed]
        for id_ in precomputed_xns_ids:
            graph.remove_node(id_)

        # 0.4 create variables related to futures
        futures: Dict[IdentityHash, "Future[Any]"] = {}
        done: Set["Future[Any]"] = set()
        running: Set["Future[Any]"] = set()

        # TODO: support non "threadable" ExecNodes.
        #  These are ExecNodes that can't run inside a thread because their arguments aren't pickelable!

        # 0.5 create helpers functions encapsulated from the outside
        def get_num_running_threads(_futures: Dict[IdentityHash, "Future[Any]"]) -> int:
            # use not future.done() because there is no guarantee that Thread pool will directly execute
            # the submitted thread
            return sum([not future.done() for future in _futures.values()])

        def get_highest_priority_nodes(nodes: List[ExecNode]) -> List[ExecNode]:
            highest_priority = max(node.priority for node in nodes)
            return [node for node in nodes if node.priority == highest_priority]

        # 0.6 get the candidates root nodes that can be executed
        # runnable_nodes_ids will be empty if all root nodes are running
        runnable_xns_ids = graph.root_nodes()

        with ThreadPoolExecutor(
            max_workers=self.max_concurrency, thread_name_prefix=call_id
        ) as executor:
            while len(graph):
                # attempt to run **A SINGLE** root node #

                # 6. block scheduler execution if no root node can be executed.
                #    this can occur in two cases:
                #       1. if maximum concurrency is reached
                #       2. if no runnable node exists (i.e. all root nodes are being executed)
                #    in both cases: block until a node finishes
                #       => a new root node will be available
                num_running_threads = get_num_running_threads(futures)
                num_runnable_nodes_ids = len(runnable_xns_ids)
                if num_running_threads == self.max_concurrency or num_runnable_nodes_ids == 0:
                    # must wait and not submit any workers before a worker ends
                    # (that might create a new more prioritized node) to be executed
                    logger.debug(
                        f"Waiting for ExecNodes {running} to finish. Finished running {done}"
                    )
                    done_, running = wait(running, return_when=FIRST_COMPLETED)
                    done = done.union(done_)

                # 1. among the finished futures:
                #       1. checks for exceptions
                #       2. and remove them from the graph
                for id_, fut in futures.items():
                    if fut.done() and id_ in graph:
                        logger.debug(f"Remove ExecNode {id_} from the graph")
                        self._handle_exception(graph, fut, id_)
                        graph.remove_node(id_)

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
                if xn.is_sequential and num_running_threads != 0:
                    logger.debug(
                        f"{xn.id} must not run in parallel."
                        f"Wait for the end of a node in {running}"
                    )
                    done_, running = wait(running, return_when=FIRST_COMPLETED)
                    # go to step 6
                    continue

                # 5.1 submit the exec node to the executor
                # TODO: make a special case if self.max_concurrency == 1
                #   then invoke the function directly instead of launching a thread
                exec_future = executor.submit(xn.execute, node_dict=xns_dict)
                running.add(exec_future)
                futures[xn.id] = exec_future

                # 5.2 wait for the sequential node to finish
                # TODO: not sure this code ever runs
                if xn.is_sequential:
                    wait(futures.values(), return_when=ALL_COMPLETED)
        return xns_dict

    def _alias_to_ids(self, alias: Alias) -> List[IdentityHash]:
        """Extract an ExecNode ID from an Alias (Tag, ExecNode ID or ExecNode)

        Args:
            alias (Alias): an Alias (Tag, ExecNode ID or ExecNode)

        Returns:
            The corresponding ExecNode IDs

        Raises:
            ValueError: if a requested ExecNode is not found in the DAG
            TawaziTypeError: if the Type of the identifier is not Tag, IdentityHash or ExecNode
        """
        if isinstance(alias, ExecNode):
            return [alias.id]
        # todo: do further validation for the case of the tag!!
        elif isinstance(alias, (IdentityHash, tuple)):
            # if leaves_identification is not ExecNode, it can be either
            #  1. a Tag (Highest priority in case an id with the same value exists)
            if nodes := self.tagged_nodes.get(alias):
                return [node.id for node in nodes]
            #  2. or a node id!
            elif isinstance(alias, IdentityHash) and alias in self.node_dict:
                node = self.get_node_by_id(alias)
                return [node.id]
            else:
                raise ValueError(
                    f"node or tag {alias} not found in DAG.\n"
                    f" Available nodes are {self.node_dict}.\n"
                    f" Available tags are {list(self.tagged_nodes.keys())}"
                )
        else:
            raise TawaziTypeError(
                "target_nodes must be of type ExecNode, "
                f"str or tuple identifying the node but provided {alias}"
            )

    # NOTE: this function is named wrongly!
    def _get_target_ids(self, target_nodes: List[Alias]) -> List[IdentityHash]:
        """
        get the ids of ExecNodes corresponding to target_nodes.
        The identification can be carried out using the tag, the Id, or the ExecNode itself.
        Keep in Mind that depending on the way ExecNode is provided inside target_nodes,
         the returned id
        if nothing is provided it will return all leaves_ids
        Handles the debug nodes

        Args:
            target_nodes (Optional[List[Alias]]): list of a ExecNode Aliases that the user might provide to run a subgraph

        Returns:
            List[IdentityHash]: Leaf ExecNodes' Identities
        """
        # Raises:
        #     TawaziBaseException: if the returned List[IdentityHash] has the wrong length, this indicates a bug in the code

        target_ids = list(chain(*(self._alias_to_ids(alias) for alias in target_nodes)))

        return target_ids

    def _extend_leaves_ids_debug_xns(self, leaves_ids: List[IdentityHash]) -> List[IdentityHash]:
        new_debug_xn_discovered = True
        while new_debug_xn_discovered:
            new_debug_xn_discovered = False
            for id_ in leaves_ids:
                for successor_id in self.frwrd_deps[id_]:
                    is_successor_debug = self.node_dict[successor_id].debug
                    if successor_id not in leaves_ids and is_successor_debug:
                        # a new debug XN has been discovered!
                        new_debug_xn_discovered = True
                        preds_of_succs_ids = [xn_id for xn_id in self.bckrd_deps[successor_id]]

                        if set(preds_of_succs_ids).issubset(set(leaves_ids)):
                            # this new XN can run by only running the current leaves_ids
                            leaves_ids.append(successor_id)
        return leaves_ids

    def setup(
        self,
        target_nodes: Optional[List[Alias]] = None,
        exclude_nodes: Optional[List[Alias]] = None,
    ) -> None:
        """Run the setup ExecNodes for the DAG.

        If target_nodes are provided, run only the necessary setup ExecNodes, otherwise will run all setup ExecNodes.
        NOTE: currently if setup ExecNodes receive arguments from the Pipeline this method won't work because it doesn't support *args.
            This might be supported in the future though

        Args:
            target_nodes (Optional[List[XNId]], optional): The ExecNodes that the user aims to use in the DAG.
                This might include setup or non setup ExecNodes. If None is provided, will run all setup ExecNodes. Defaults to None.
            exclude_nodes (Optional[List[XNId]], optional): The ExecNodes that the user aims to exclude from the DAG.
                The user is responsible for ensuring that the overlapping between the target_nodes and exclude_nodes is logical.
        """

        # 1. select all setup ExecNodes
        #  do not copy the setup nodes because we want them to be modified per DAG instance!
        all_setup_nodes = {
            nd.id: nd
            for nd in self.exec_nodes
            if nd.setup or (isinstance(nd, ArgExecNode) and nd.executed)
        }

        # 2. if target_nodes is not provided run all setup ExecNodes
        if target_nodes is None:
            target_ids = list(all_setup_nodes.keys())
            graph = self._make_subgraph(target_ids, exclude_nodes)  # type: ignore

        else:
            # 2.1 the leaves_ids that the user wants to execute
            #  however they might contain non setup nodes... so we should extract all the nodes ids
            #  that must be run in order to run the target_nodes ExecNodes
            #  afterwards we can remove the non setup nodes
            target_ids = self._get_target_ids(target_nodes)

            # 2.2 filter non setup ExecNodes
            graph = self._make_subgraph(target_ids, exclude_nodes)  # type: ignore
            ids_to_remove = [id_ for id_ in graph if id_ not in all_setup_nodes]

            for id_ in ids_to_remove:
                graph.remove_node(id_)
        # TODO: handle debug XNs!

        self._execute(graph, all_setup_nodes)

    def executor(self, **kwargs: Dict[str, Any]) -> "DAGExecution":
        """Generates an executor for the DAG.

        Args:
            **kwargs (Dict[str, Any]): keyword arguments to be passed to DAGExecution's constructor

        Returns:
            DAGExecution: an executor for the DAG
        """
        return DAGExecution(self, **kwargs)  # type: ignore

    def _make_subgraph(
        self,
        target_nodes: Optional[List[Alias]] = None,
        exclude_nodes: Optional[List[Alias]] = None,
    ) -> nx.DiGraph:
        graph = deepcopy(self.graph_ids)

        if target_nodes is not None:
            target_ids = self._get_target_ids(target_nodes)
            graph.subgraph_leaves(target_ids)

        if exclude_nodes is not None:
            exclude_ids = list(chain(*(self._alias_to_ids(alias) for alias in exclude_nodes)))

            for id_ in exclude_ids:
                # maybe previously removed by :
                # 1. not being inside the subgraph
                # 2. being a successor of an excluded node
                if id_ in graph:
                    graph.remove_recursively(id_)

        if target_nodes and exclude_nodes:
            for id_ in target_ids:
                if id_ not in graph:
                    raise TawaziUsageError(
                        f"target_nodes include {id_} which is removed by exclude_nodes: {exclude_ids}, "
                        f"please verify that they don't overlap in a non logical way!"
                    )

        # handle debug nodes
        if Cfg.RUN_DEBUG_NODES:
            leaves_ids = graph.leaf_nodes
            # after extending leaves_ids, we should do a recheck because this might recreate another debug-able XN...
            target_ids = self._extend_leaves_ids_debug_xns(leaves_ids)

            # extend the graph with the debug XNs
            # This is not efficient but it is ok since we are debugging the code anyways
            debug_graph = deepcopy(self.graph_ids)
            debug_graph.subgraph_leaves(list(graph.nodes) + target_ids)
            graph = debug_graph

        # 3. clean all debug XNs if they shouldn't run!
        else:
            to_remove = [id_ for id_ in graph if self.node_dict[id_].debug]
            for id_ in to_remove:
                graph.remove_node(id_)

        return graph

    def __call__(
        self,
        *args: Any,
        target_nodes: Optional[List[Alias]] = None,
        exclude_nodes: Optional[List[Alias]] = None,
    ) -> Any:
        """
        Execute the DAG scheduler via a similar interface to the function that describes the dependencies.

        Args:
            *args (Any): arguments to be passed to the call of the DAG
            target_nodes (Optional[List[XNId]], optional): target ExecNodes to execute
                executes the whole DAG if None. Defaults to None.
            exclude_nodes (Optional[List[XNId]], optional): The ExecNodes that the user aims to exclude from the DAG.
                The user is responsible for ensuring that the overlapping between the target_nodes and exclude_nodes is logical.


        Returns:
            Any: return value of the DAG's execution
        """
        # Raises:
        #     TawaziTypeError: if target_nodes contains a wrong typed identifier or if the return value contain a non LazyExecNode

        # 1. generate the subgraph to be executed
        graph = self._make_subgraph(target_nodes=target_nodes, exclude_nodes=exclude_nodes)

        # 2. copy the ExecNodes
        call_xn_dict = self._make_call_xn_dict(*args)

        # 3. Execute the scheduler
        all_nodes_dict = self._execute(graph, call_xn_dict)

        # 4. extract the returned value/values
        returned_values = self._get_return_values(all_nodes_dict)

        return returned_values

    def _make_call_xn_dict(self, *args: Any) -> Dict[IdentityHash, ExecNode]:
        """
        Generate the calling ExecNode dict.
        This is a dict containing ExecNodes that will be executed (hence modified) by the DAG scheduler.
        This takes into consideration:
         1. deep copying the ExecNodes
         2. filling the arguments of the call
         3. skipping the copy for setup ExecNodes

        Args:
            *args (Any): arguments to be passed to the call of the DAG

        Returns:
            Dict[IdentityHash, ExecNode]: The modified ExecNode dict which will be executed by the DAG scheduler.

        Raises:
            TypeError: If called with an invalid number of arguments
        """
        # 1. deepcopy the node_dict because it will be modified by the DAG's execution
        call_xn_dict = DAG._copy_non_setup_xns(self.node_dict)

        # 2. parse the input arguments of the pipeline
        # 2.1 default valued arguments can be skipped and not provided!
        # note: if not enough arguments are provided then the code will fail
        # inside the DAG's execution through the raise_err lambda
        if args:
            # 2.2 can't provide more than enough arguments
            if len(args) > len(self.input_ids):
                raise TypeError(
                    f"The DAG takes a maximum of {len(self.input_ids)} arguments. {len(args)} arguments provided"
                )

            # 2.3 modify ExecNodes corresponding to input ArgExecNodes
            for ind_arg, arg in enumerate(args):
                node_id = self.input_ids[ind_arg]

                call_xn_dict[node_id].result = arg

        return call_xn_dict

    def _get_return_values(
        self, xn_dict: Dict[IdentityHash, ExecNode]
    ) -> Union[Any, Tuple[Any], List[Any]]:
        """
        Extract the return value/values from the output of the DAG's scheduler!

        Args:
            xn_dict (Dict[IdentityHash, ExecNode]): _description_

        Raises:
            TawaziTypeError: _description_

        Returns:
            Union[Any, Tuple[Any], List[Any]]: _description_
        """
        if self.return_ids is None:
            return None
        if isinstance(self.return_ids, IdentityHash):
            return filter_NoVal(xn_dict[self.return_ids].result)
        if isinstance(self.return_ids, (tuple, list)):
            gen = (filter_NoVal(xn_dict[ren_id].result) for ren_id in self.return_ids)
            if isinstance(self.return_ids, tuple):
                return tuple(gen)
            if isinstance(self.return_ids, list):
                return list(gen)
        if isinstance(self.return_ids, dict):
            return {
                key: filter_NoVal(xn_dict[ren_id].result) for key, ren_id in self.return_ids.items()
            }

        raise TawaziTypeError("Return type for the DAG can only be a single value, Tuple or List")

    # NOTE: this function should be used in case there was a bizarre behavior noticed during
    #   the execution of the DAG via DAG.execute(...)
    def _safe_execute(
        self,
        *args: Any,
        target_nodes: Optional[List[Alias]] = None,
        exclude_nodes: Optional[List[Alias]] = None,
    ) -> Any:
        """
        Execute the ExecNodes in topological order without priority in for loop manner for debugging purposes

        Args:
            *args (Any): Positional arguments passed to the DAG
            target_nodes (Optional[List[Alias]]): the ExecNodes that should be considered to construct the subgraph
            exclude_nodes (Optional[List[Alias]]): the ExecNodes that shouldn't run

        Returns:
            Any: the result of the execution of the DAG.
             If an ExecNode returns a value in the DAG but is not executed, it will return None.
        """
        # 1. make the graph_ids to be executed!
        graph = self._make_subgraph(target_nodes, exclude_nodes)

        # 2. make call_xn_dict that will be modified
        call_xn_dict = self._make_call_xn_dict(*args)

        # 3. deep copy the node_dict to store the results in each node
        for xn_id in graph.topologically_sorted:
            # only execute ExecNodes that are part of the subgraph
            call_xn_dict[xn_id].execute(call_xn_dict)

        # 4. make returned values
        return_values = self._get_return_values(call_xn_dict)

        return return_values

    def _handle_exception(self, graph: DiGraphEx, fut: "Future[Any]", id_: IdentityHash) -> None:
        """
        checks if futures have produced exceptions, and handles them
        according to the specified behavior

        Args:
            graph: the graph
            fut: the thread future
            id_: the IdentityHash of the current ExecNode

        Raises:
            NotImplementedError: if self.behavior is not known
        """

        if self.behavior == ErrorStrategy.strict:
            # will raise the first encountered exception if there's one
            # no simpler way to check for exception, and not supported by flake8
            _res = fut.result()  # noqa: F841

        else:
            try:
                _res = fut.result()  # noqa: F841

            except Exception:
                logger.exception(f"The feature {id_} encountered the following error:")

                if self.behavior == ErrorStrategy.permissive:
                    logger.warning("Ignoring exception as the behavior is set to permissive")

                elif self.behavior == ErrorStrategy.all_children:
                    # remove all its children. Current node will be removed directly afterwards
                    successors = list(graph.successors(id_))
                    for children_ids in successors:
                        # TODO: implement a test for all_children! it should fail!
                        # Afterwards include parameter to remove the node itself or not
                        graph.remove_recursively(children_ids)

                else:
                    raise NotImplementedError(f"Unknown behavior name: {self.behavior}")

    def config_from_dict(self, config: Dict[str, Any]) -> None:
        """
        Allows reconfiguring the parameters of the nodes from a dictionary

        Args:
            config (Dict[str, Any]): the dictionary containing the config
                    example: {"nodes": {"a": {"priority": 3, "is_sequential": True}}, "max_concurrency": 3}

        Raises:
            ValueError: if two nodes are configured by the provided config (which is ambiguous)
        """

        def _override_node_config(n: ExecNode, cfg: Dict[str, Any]) -> bool:
            flag = False
            if "is_sequential" in cfg:
                n.is_sequential = cfg["is_sequential"]

            if "priority" in cfg:
                n.priority = cfg["priority"]
                flag = True

            return flag

        prio_flag = False
        visited: Dict[str, Any] = {}
        if "nodes" in config:
            for alias, conf_node in config["nodes"].items():
                ids_ = self._alias_to_ids(alias)
                for node_id in ids_:
                    if node_id not in visited:
                        node = self.get_node_by_id(node_id)
                        node_prio_flag = _override_node_config(node, conf_node)
                        prio_flag = node_prio_flag or prio_flag  # keep track of flag

                    else:
                        raise ValueError(
                            f"trying to set two configs for node {node_id}.\n 1) {visited[node_id]}\n 2) {conf_node}"
                        )

                    visited[node_id] = conf_node

        if "max_concurrency" in config:
            self.max_concurrency = config["max_concurrency"]

        if prio_flag:
            # if we changed the priority of some nodes we need to recompute the compound prio
            self._assign_compound_priority(self.graph_ids, self.node_dict, self.bckrd_deps)

    def config_from_yaml(self, config_path: str) -> None:
        """
        Allows reconfiguring the parameters of the nodes from a yaml file

        Args:
            config_path: the path to the yaml file
        """
        with open(config_path, "r") as f:
            yaml_config = yaml.load(f, Loader=UniqueKeyLoader)

        self.config_from_dict(yaml_config)

    def config_from_json(self, config_path: str) -> None:
        """
        Allows reconfiguring the parameters of the nodes from a yaml file

        Args:
            config_path: the path to the json file
        """
        with open(config_path, "r") as f:
            json_config = json.load(f)

        self.config_from_dict(json_config)


# TODO: should implement twz_exclude_nodes
# TODO: check if the arguments are the same, then run the DAG using the from_cache.
#  If the arguments are not the same, then rerun the DAG!
class DAGExecution:
    def __init__(
        self,
        dag: DAG,
        target_nodes: Optional[List[Alias]] = None,
        exclude_nodes: Optional[List[Alias]] = None,
        cache_in: str = "",
        from_cache: str = "",
        call_id: Optional[str] = None,
    ):
        """
        This is an instance of DAGExecution which is a disposable callable instance of a DAG.
        It holds information about the DAG's last execution. Hence it is not threadsafe.
        It is reusable, however it is not recommended to reuse an instance of DAGExecutor!.

        Args:
            dag (DAG): The attached DAG.
            target_nodes (Optional[List[Alias]]): The leave ExecNodes to execute.
                If None will execute all ExecNodes.
                Defaults to None.
            exclude_nodes (Optional[List[Alias]]): The leave ExecNodes to exclude.
                If None will exclude all ExecNodes.
                Defaults to None.
            cache_in (str):
                the path to the file where the execution should be cached.
                The path should end in `.pkl`.
                Will skip caching if `cache_in` is Falsy.
                Will raise PickleError if any of the values passed around in the DAG is not pickleable.
                Defaults to "".
            from_cache (str):
                the path to the file where the execution should be loaded from.
                The path should end in `.pkl`.
                Will skip loading from cache if `from_cache` is Falsy.
                Defaults to "".
            call_id (Optional[str]): identification of the current execution.
                This will be inserted into thread_name_prefix while executing the threadPool.
                It will be used in the future for identifying the execution inside Processes etc.
        """
        # todo: Maybe we can support .dill to extend the possibilities of the exchanged values, but this won't solve the whole problem

        self.dag = dag
        self.target_nodes = target_nodes
        self.exclude_nodes = exclude_nodes
        self.cache_in = cache_in
        self.from_cache = from_cache
        # NOTE: from_cache is orthogonal to cache_in which means that if cache_in is set at the same time as from_cache.
        #  in this case the DAG will be loaded from_cache and the results will be saved again to the cache_in file.
        self.call_id = call_id

        # get the leaves ids to execute in case of a subgraph
        self.target_nodes = target_nodes
        self.exclude_nodes = exclude_nodes

        self.xn_dict: Dict[IdentityHash, ExecNode] = {}
        self.results: Dict[IdentityHash, Any] = {}

    @property
    def cache_in(self) -> str:
        return self._cache_in

    @cache_in.setter
    def cache_in(self, cache_in: str) -> None:
        if cache_in and not cache_in.endswith(".pkl"):
            raise ValueError("cache_in should end with.pkl")
        self._cache_in = cache_in

    @property
    def from_cache(self) -> str:
        return self._from_cache

    @from_cache.setter
    def from_cache(self, from_cache: str) -> None:
        if from_cache and not from_cache.endswith(".pkl"):
            raise ValueError("from_cache should end with.pkl")
        self._from_cache = from_cache

    # we need to reimplement the public methods of DAG here in order to have a constant public interface
    # getters
    def get_nodes_by_tag(self, tag: Any) -> List[ExecNode]:
        nodes = [ex_n for ex_n in self.xn_dict.values() if ex_n.tag == tag]
        return nodes

    def get_node_by_id(self, id_: IdentityHash) -> ExecNode:
        # TODO: ? catch the keyError and
        #   help the user know the id of the ExecNode by pointing to documentation!?
        return self.xn_dict[id_]

    def setup(self, twz_nodes: Optional[List[Alias]] = None) -> None:
        """Does the same thing as DAG.setup

        Args:
            twz_nodes (Optional[List[Alias]], optional): c.f. `DAG.setup`.
        """
        self.dag.setup(twz_nodes)

    def __call__(self, *args: Any) -> Any:
        # NOTE: *args will be ignored if self.from_cache is set!
        dag = self.dag
        graph = dag._make_subgraph(self.target_nodes, self.exclude_nodes)

        # maybe call_id will be changed to Union[int, str].
        # Keep call_id as Optional[str] for now
        call_id = self.call_id if self.call_id is not None else ""

        # 1. copy the ExecNodes
        call_xn_dict = dag._make_call_xn_dict(*args)
        if self.from_cache:
            with open(self.from_cache, "rb") as f:
                cached_results = pickle.load(f)
            # set the result for the ExecNode that were previously executed
            # this will make them skip execution inside the scheduler
            for id_, result in cached_results.items():
                call_xn_dict[id_].result = result

        # 2. Execute the scheduler
        self.xn_dict = dag._execute(graph, call_xn_dict, call_id)
        self.results = {xn.id: xn.result for xn in self.xn_dict.values()}

        # 3. cache in the results
        if self.cache_in:
            Path(self.cache_in).parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_in, "wb") as f:
                # NOTE: we are currently only storing the results of the execution,
                #  this means that the configuration of the ExecNodes are lost!
                #  But this is ok since it should not change between executions!
                #  for example, a setup ExecNode should stay a setup ExecNode between caching in the results and reading back the cached results
                #  the same goes for the DAG itself, the behavior when an error is encountered & its concurrency will be controlled via the constructor

                pickle.dump(self.results, f, protocol=pickle.HIGHEST_PROTOCOL, fix_imports=False)

        # 3. extract the returned value/values
        returned_values = dag._get_return_values(self.xn_dict)

        return returned_values

    # TODO: add execution order (the order in which the nodes were executed)
