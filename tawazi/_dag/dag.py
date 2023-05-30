"""module containing DAG and DAGExecution which are the containers that run ExecNodes in Tawazi."""
import json
import pickle
import time
import warnings
from collections import defaultdict
from copy import copy, deepcopy
from itertools import chain
from pathlib import Path
from typing import Any, Dict, Generic, List, NoReturn, Optional, Sequence, Set, Union

import networkx as nx
import yaml
from loguru import logger
from networkx.exception import NetworkXUnfeasible

from tawazi._helpers import _make_raise_arg_error, _UniqueKeyLoader
from tawazi.config import cfg
from tawazi.consts import RVDAG, Identifier, NoVal, P, RVTypes, Tag
from tawazi.errors import ErrorStrategy, TawaziTypeError, TawaziUsageError
from tawazi.node import Alias, ArgExecNode, ExecNode, ReturnUXNsType, UsageExecNode

from .digraph import DiGraphEx
from .helpers import copy_non_setup_xns, execute


class DAG(Generic[P, RVDAG]):
    """Data Structure containing ExecNodes with interdependencies.

    Please do not instantiate this class directly. Use the decorator `@dag` instead.
    The ExecNodes can be executed in parallel with the following restrictions:
        * Limited number of threads.
        * Parallelization constraint of each ExecNode (is_sequential attribute)
    """

    def __init__(
        self,
        exec_nodes: Dict[Identifier, ExecNode],
        input_uxns: List[UsageExecNode],
        return_uxns: ReturnUXNsType,
        max_concurrency: int = 1,
        behavior: ErrorStrategy = ErrorStrategy.strict,
    ):
        """Constructor of the DAG. Should not be called directly. Instead use the `dag` decorator.

        Args:
            exec_nodes: all the ExecNodes
            input_uxns: all the input UsageExecNodes
            return_uxns: the return UsageExecNodes. These can be of various types: None, a single value, tuple, list, dict.
            max_concurrency: the maximal number of threads running in parallel
            behavior: specify the behavior if an ExecNode raises an Error. Three option are currently supported:
                1. DAG.STRICT: stop the execution of all the DAG
                2. DAG.ALL_CHILDREN: do not execute all children ExecNodes, and continue execution of the DAG
                2. DAG.PERMISSIVE: continue execution of the DAG and ignore the error
        """
        self.max_concurrency = max_concurrency
        self.behavior = behavior
        self.return_uxns = return_uxns
        self.input_uxns = input_uxns

        # ExecNodes can be shared between Graphs, their call signatures might also be different
        # NOTE: maybe this should be transformed into a property because there is a deepcopy for node_dict...
        #  this means that there are different ExecNodes that are hanging arround in the same instance of the DAG
        self.node_dict = exec_nodes
        # Compute all the tags in the DAG to reduce overhead during computation
        self.tagged_nodes = defaultdict(list)
        for xn in self.node_dict.values():
            if xn.tag:
                if isinstance(xn.tag, Tag):
                    self.tagged_nodes[xn.tag].append(xn)
                # isinstance(xn.tag, tuple):
                else:
                    for t in xn.tag:
                        self.tagged_nodes[t].append(xn)

        # Might be useful in the future
        self.node_dict_by_name: Dict[str, ExecNode] = {
            exec_node.__name__: exec_node for exec_node in self.node_dict.values()
        }

        self.graph_ids = self._build_graph()

        self.bckrd_deps = {
            id_: list(self.graph_ids.predecessors(xn.id)) for id_, xn in self.node_dict.items()
        }
        self.frwrd_deps = {
            id_: list(self.graph_ids.successors(xn.id)) for id_, xn in self.node_dict.items()
        }

        # calculate the sum of priorities of all recursive children
        self._assign_compound_priority()

        # make a valid execution sequence to run sequentially if needed
        topological_order = self.graph_ids.topologically_sorted
        self.exec_node_sequence = [self.node_dict[xn_id] for xn_id in topological_order]

        self._validate()

    @property
    def max_concurrency(self) -> int:
        """Maximal number of threads running in parallel. (will change!)."""
        return self._max_concurrency

    @max_concurrency.setter
    def max_concurrency(self, value: int) -> None:
        """Set the maximal number of threads running in parallel.

        Args:
            value (int): maximum number of threads running in parallel

        Raises:
            ValueError: if value is not a positive integer
        """
        if not isinstance(value, int):
            raise ValueError("max_concurrency must be an int")
        if value < 1:
            raise ValueError("Invalid maximum number of threads! Must be a positive integer")
        self._max_concurrency = value

    # getters
    def get_nodes_by_tag(self, tag: Tag) -> List[ExecNode]:
        """Get the ExecNodes with the given tag.

        Note: the returned ExecNode is not modified by any execution!
            This means that you can not get the result of its execution via `DAG.get_nodes_by_tag(<tag>).result`.
            In order to do that, you need to make a DAGExecution and then call `DAGExecution.get_nodes_by_tag(<tag>).result`, which will contain the results.

        Args:
            tag (Any): tag of the ExecNodes

        Returns:
            List[ExecNode]: corresponding ExecNodes
        """
        if isinstance(tag, Tag):
            return self.tagged_nodes[tag]
        raise TypeError(f"tag {tag} must be of Tag type. Got {type(tag)}")

    def get_node_by_id(self, id_: Identifier) -> ExecNode:
        """Get the ExecNode with the given id.

        Note: the returned ExecNode is not modified by any execution!
            This means that you can not get the result of its execution via `DAG.get_node_by_id(<id>).result`.
            In order to do that, you need to make a DAGExecution and then call `DAGExecution.get_node_by_id(<id>).result`, which will contain the results.

        Args:
            id_ (Identifier): id of the ExecNode

        Returns:
            ExecNode: corresponding ExecNode
        """
        # TODO: ? catch the keyError and
        #   help the user know the id of the ExecNode by pointing to documentation!?
        return self.node_dict[id_]

    def _get_single_xn_by_alias(self, alias: Alias) -> ExecNode:
        """Get the ExecNode corresponding to the given Alias.

        Args:
            alias (Alias): the Alias to be resolved

        Raises:
            ValueError: if the Alias is not unique

        Returns:
            ExecNode: the ExecNode corresponding to the given Alias
        """
        xns = self._alias_to_ids(alias)
        if len(xns) > 1:
            raise ValueError(
                f"Alias {alias} is not unique. It points to {len(xns)} ExecNodes: {xns}"
            )
        return self.node_dict[xns[0]]

    # TODO: get node by usage (the order of call of an ExecNode)

    # TODO: implement None for outputs to indicate a None output ? (this is not a prioritized feature)
    # TODO: implement ellipsis for composing for the input & outputs
    # TODO: should we support kwargs when DAG.__call__ support kwargs?
    # TODO: Maybe insert an ID into DAG that is related to the dependency describing function !? just like ExecNode
    #  This will be necessary when we want to make a DAG containing DAGs besides ExecNodes
    # NOTE: by doing this, we create a new ExecNode for each input.
    #  Hence we loose all information related to the original ExecNode (tags, etc.)
    #  Maybe a better way to do this is to transform the original ExecNode into an ArgExecNode

    def compose(self, inputs: Union[Alias, Sequence[Alias]], outputs: Union[Alias, Sequence[Alias]], **kwargs: Dict[str, Any]) -> "DAG":  # type: ignore[type-arg]
        """Compose a new DAG using inputs and outputs ExecNodes (Experimental).

        All provided `Alias`es must point to unique `ExecNode`s. Otherwise ValueError is raised
        The user is responsible to correctly specify inputs and outputs signature of the `DAG`.
        * The inputs can be specified as a single `Alias` or a `Sequence` of `Alias`es.
        * The outputs can be specified as a single `Alias` (a single value is returned)
        or a `Sequence` of `Alias`es in which case a Tuple of the values are returned.
        If outputs are specified as [], () is returned.
        The syntax is the following:
        >>> from tawazi import dag, xn, DAG
        >>> from typing import Tuple, Any
        >>> @xn
        ... def unwanted_xn() -> int: return 42
        >>> @xn
        ... def x(v: Any) -> int: return int(v)
        >>> @xn
        ... def y(v: Any) -> str: return str(v)
        >>> @xn
        ... def z(x: int, y: str) -> float: return float(x) + float(y)
        >>> @dag
        ... def pipe() -> Tuple[int, float, int]:
        ...     a = unwanted_xn()
        ...     res = z(x(1), y(1))
        ...     b = unwanted_xn()
        ...     return a, res, b
        >>> composed_dag = pipe.compose([x, y], z)
        >>> assert composed_dag(1, 1) == 2.0
        >>> # composed_dag: DAG[[int, str], float] = pipe.compose([x, y], [z])  # optional typing of the returned DAG!
        >>> # assert composed_dag(1, 1) == 2.0  # type checked!


        Args:
            inputs (Alias | List[Alias]): the Inputs nodes whose results are provided.
            outputs (Alias | List[Alias]): the Output nodes that must execute last, The ones that will generate results
            **kwargs (Dict[str, Any]): additional arguments to be passed to the DAG's constructor
        """
        # what happens for edge cases ??
        # 1. if inputs are less than sufficient to produce outputs (-> error)
        # 2. if inputs are more than sufficient to produce outputs (-> warning)
        # 3. if inputs are successors of outputs (-> error)
        # 5. if inputs are successors of inputs but predecessors of outputs
        # 1. inputs and outputs are overlapping (-> error ambiguous ? maybe not)
        # 1. if inputs & outputs are [] (-> ())
        # 4. cst cubgraph inputs is [], outputs is not [] but contains constants (-> works as expected)
        # 5. inputs is not [], outputs is [] same as 2.
        # 6. a subcase of the above (some inputs suffice to produce some of the outputs, but some inputs don't)
        # 7. advanced usage: if inputs contain ... (i.e. Ellipsis) in this case we must expand it to reach all the remaining XN in a smart manner
        #  we should keep the order of the inputs and outputs (future)
        # 8. what if some arguments have default values? should they be provided by the user?
        # 9. how to specify that arguments of the original DAG should be provided by the user? the user should provide the input's ID which is not a stable Alias yet

        def _alias_or_aliases_to_ids(
            alias_or_aliases: Union[Alias, Sequence[Alias]]
        ) -> List[Identifier]:
            if isinstance(alias_or_aliases, str) or isinstance(alias_or_aliases, ExecNode):
                return [self._get_single_xn_by_alias(alias_or_aliases).id]
            return [self._get_single_xn_by_alias(a_id).id for a_id in alias_or_aliases]

        def _raise_input_successor_of_input(pred: Identifier, succ: Set[Identifier]) -> NoReturn:
            raise ValueError(
                f"Input ExecNodes {succ} depend on Input ExecNode {pred}."
                f"this is ambiguous. Remove either one of them."
            )

        def _raise_missing_input(input_: Identifier) -> NoReturn:
            raise ValueError(
                f"ExecNode {input_} are not declared as inputs. "
                f"Either declare them as inputs or modify the requests outputs."
            )

        def _alias_or_aliases_to_uxns(
            alias_or_aliases: Union[Alias, Sequence[Alias]]
        ) -> ReturnUXNsType:
            if isinstance(alias_or_aliases, str) or isinstance(alias_or_aliases, ExecNode):
                return UsageExecNode(self._get_single_xn_by_alias(alias_or_aliases).id)
            return tuple(
                UsageExecNode(self._get_single_xn_by_alias(a_id).id) for a_id in alias_or_aliases
            )

        # 1. get input ids and output ids.
        #  Alias should correspond to a single ExecNode,
        #  otherwise an ambiguous situation exists, raise error
        in_ids = _alias_or_aliases_to_ids(inputs)
        out_ids = _alias_or_aliases_to_ids(outputs)

        # 2.1 contains all the ids of the nodes that will be in the new DAG
        set_xn_ids = set(in_ids + out_ids)

        # 2.2 all ancestors of the inputs
        in_ids_ancestors: Set[Identifier] = self.graph_ids.ancestors_of_iter(in_ids)

        # 3. check edge cases
        # inputs should not be successors of inputs, otherwise (error)
        # and inputs should produce at least one of the outputs, otherwise (warning)
        for i in in_ids:
            # if pred is ancestor of an input, raise error
            if i in in_ids_ancestors:
                _raise_input_successor_of_input(i, set_xn_ids)

            # if i doesn't produce any of the wanted outputs, raise a warning!
            descendants: Set[Identifier] = nx.descendants(self.graph_ids, i)
            if descendants.isdisjoint(out_ids):
                warnings.warn(
                    f"Input ExecNode {i} is not used to produce any of the requested outputs."
                    f"Consider removing it from the inputs.",
                    stacklevel=2,
                )

        # 4. collect necessary ExecNodes' IDS

        # 4.1 original DAG's inputs that don't contain default values.
        # used to detect missing inputs
        dag_inputs_ids = [
            uxn.id for uxn in self.input_uxns if self.node_dict[uxn.id].result is NoVal
        ]

        # 4.2 define helper function
        def _add_missing_deps(candidate_id: Identifier, set_xn_ids: Set[Identifier]) -> None:
            """Adds missing dependency to the set of ExecNodes that will be in the new DAG.

            Note: uses nonlocal variable dag_inputs_ids

            Args:
                candidate_id (Identifier): candidate id of an `ExecNode` that will be in the new DAG
                set_xn_ids (Set[Identifier]): Set of `ExecNode`s that will be in the new DAG
            """
            preds = self.graph_ids.predecessors(candidate_id)
            for pred in preds:
                if pred not in set_xn_ids:
                    # this candidate is necessary to produce the output,
                    # it is an input to the original DAG
                    # it is not provided as an input to the composed DAG
                    # hence the user forgot to supply it! (raise error)
                    if pred in dag_inputs_ids:
                        _raise_missing_input(pred)

                    # necessary intermediate dependency.
                    # collect it in the set
                    set_xn_ids.add(pred)
                    _add_missing_deps(pred, set_xn_ids)

        # 4.3 add all required dependencies for each output
        for o_id in out_ids:
            _add_missing_deps(o_id, set_xn_ids)

        # 5.1 copy the ExecNodes that will be in the composed DAG because
        #  maybe the composed DAG will modify them (e.g. change their tags)
        #  and we don't want to modify the original DAG
        xn_dict = {xn_id: copy(self.node_dict[xn_id]) for xn_id in set_xn_ids}

        # 5.2 change the inputs of the ExecNodes into ArgExecNodes
        for xn_id, xn in xn_dict.items():
            if xn_id in in_ids:
                logger.debug("changing Composed-DAG's input {} into ArgExecNode", xn_id)
                xn.__class__ = ArgExecNode
                xn.exec_function = _make_raise_arg_error("composed", xn.id)
                # eliminate all dependencies
                xn.args = []
                xn.kwargs = {}

        # 5.3 make the inputs and outputs UXNs for the composed DAG
        in_uxns = [UsageExecNode(xn_id) for xn_id in in_ids]
        # if a single value is returned make the output a single value
        out_uxns = _alias_or_aliases_to_uxns(outputs)

        # 6. return the composed DAG
        # ignore[arg-type] because the type of the kwargs is not known
        return DAG(xn_dict, in_uxns, out_uxns, **kwargs)  # type: ignore[arg-type]

    def _build_graph(self) -> DiGraphEx:
        """Builds the graph and the sequence order for the computation.

        Raises:
            NetworkXUnfeasible: if the graph has cycles
        """
        graph_ids = DiGraphEx()
        # 1. Make the graph
        # 1.1 add nodes
        for id_ in self.node_dict.keys():
            graph_ids.add_node(id_)

        # 1.2 add edges
        for xn in self.node_dict.values():
            edges = [(dep.id, xn.id) for dep in xn.dependencies]
            graph_ids.add_edges_from(edges)

        # 2. Validate the DAG: check for circular dependencies
        cycle = graph_ids._find_cycle()
        if cycle:
            raise NetworkXUnfeasible(
                f"the product contains at least a circular dependency: {cycle}"
            )
        return graph_ids

    def _validate(self) -> None:
        input_ids = [uxn.id for uxn in self.input_uxns]
        # validate setup ExecNodes
        for xn in self.node_dict.values():
            if xn.setup and any(dep.id in input_ids for dep in xn.dependencies):
                raise TawaziUsageError(
                    f"The ExecNode {xn} takes as parameters one of the DAG's input parameter"
                )
        # future validations...

    def _assign_compound_priority(self) -> None:
        """Assigns a compound priority to all nodes in the graph.

        The compound priority is the sum of the priorities of all children recursively.
        """
        # 1. deepcopy graph_ids because it will be modified (pruned)
        graph_ids = deepcopy(self.graph_ids)
        leaf_ids = graph_ids.leaf_nodes

        # 2. assign the compound priority for all the remaining nodes in the graph:
        # Priority assignment happens by epochs:
        # 2.1. during every epoch, we assign the compound priority for the parents of the current leaf nodes
        # 2.2. at the end of every epoch, we trim the graph from its leaf nodes;
        #       hence the previous parents become the new leaf nodes
        while len(graph_ids) > 0:
            # Epoch level
            for leaf_id in leaf_ids:
                leaf_node = self.node_dict[leaf_id]

                for parent_id in self.bckrd_deps[leaf_id]:
                    # increment the compound_priority of the parent node by the leaf priority
                    parent_node = self.node_dict[parent_id]
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

    def _execute(
        self,
        graph: DiGraphEx,
        modified_node_dict: Optional[Dict[str, ExecNode]] = None,
        call_id: str = "",
    ) -> Dict[Identifier, Any]:
        return execute(
            node_dict=self.node_dict,
            max_concurrency=self.max_concurrency,
            behavior=self.behavior,
            graph=graph,
            modified_node_dict=modified_node_dict,
            call_id=call_id,
        )

    def _alias_to_ids(self, alias: Alias) -> List[Identifier]:
        """Extract an ExecNode ID from an Alias (Tag, ExecNode ID or ExecNode).

        Args:
            alias (Alias): an Alias (Tag, ExecNode ID or ExecNode)

        Returns:
            The corresponding ExecNode IDs

        Raises:
            ValueError: if a requested ExecNode is not found in the DAG
            TawaziTypeError: if the Type of the identifier is not Tag, Identifier or ExecNode
        """
        if isinstance(alias, ExecNode):
            if alias.id not in self.node_dict:
                raise ValueError(f"ExecNode {alias} not found in DAG")
            return [alias.id]
        # todo: do further validation for the case of the tag!!
        if isinstance(alias, (Identifier, tuple)):
            # if leaves_identification is not ExecNode, it can be either
            #  1. a Tag (Highest priority in case an id with the same value exists)
            nodes = self.tagged_nodes.get(alias)
            if nodes:
                return [node.id for node in nodes]
            #  2. or a node id!
            if isinstance(alias, Identifier) and alias in self.node_dict:
                node = self.get_node_by_id(alias)
                return [node.id]
            raise ValueError(
                f"node or tag {alias} not found in DAG.\n"
                f" Available nodes are {self.node_dict}.\n"
                f" Available tags are {list(self.tagged_nodes.keys())}"
            )
        raise TawaziTypeError(
            "target_nodes must be of type ExecNode, "
            f"str or tuple identifying the node but provided {alias}"
        )

    # NOTE: this function is named wrongly!
    def _get_target_ids(self, target_nodes: Sequence[Alias]) -> List[Identifier]:
        """Get the ids of ExecNodes corresponding to target_nodes.

        Args:
            target_nodes (Optional[List[Alias]]): list of a ExecNode Aliases that the user might provide to run a subgraph

        Returns:
            List[Identifier]: Leaf ExecNodes' Identities
        """
        return list(chain(*(self._alias_to_ids(alias) for alias in target_nodes)))

    def _extend_leaves_ids_debug_xns(self, leaves_ids: List[Identifier]) -> List[Identifier]:
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
        target_nodes: Optional[Sequence[Alias]] = None,
        exclude_nodes: Optional[Sequence[Alias]] = None,
    ) -> None:
        """Run the setup ExecNodes for the DAG.

        If target_nodes are provided, run only the necessary setup ExecNodes, otherwise will run all setup ExecNodes.
        NOTE: `DAG` arguments should not be passed to setup ExecNodes.
            Only pass in constants or setup `ExecNode`s results.


        Args:
            target_nodes (Optional[List[XNId]], optional): The ExecNodes that the user aims to use in the DAG.
                This might include setup or non setup ExecNodes. If None is provided, will run all setup ExecNodes. Defaults to None.
            exclude_nodes (Optional[List[XNId]], optional): The ExecNodes that the user aims to exclude from the DAG.
                The user is responsible for ensuring that the overlapping between the target_nodes and exclude_nodes is logical.
        """
        # 1. select all setup ExecNodes
        #  do not copy the setup nodes because we want them to be modified per DAG instance!
        all_setup_nodes = {
            id_: xn
            for id_, xn in self.node_dict.items()
            if xn.setup or (isinstance(xn, ArgExecNode) and xn.executed)
        }

        # 2. if target_nodes is not provided run all setup ExecNodes
        if target_nodes is None:
            target_ids = list(all_setup_nodes.keys())
            graph = self._make_subgraph(target_ids, exclude_nodes)

        else:
            # 2.1 the leaves_ids that the user wants to execute
            #  however they might contain non setup nodes... so we should extract all the nodes ids
            #  that must be run in order to run the target_nodes ExecNodes
            #  afterwards we can remove the non setup nodes
            target_ids = self._get_target_ids(target_nodes)

            # 2.2 filter non setup ExecNodes
            graph = self._make_subgraph(target_ids, exclude_nodes)
            ids_to_remove = [id_ for id_ in graph if id_ not in all_setup_nodes]

            for id_ in ids_to_remove:
                graph.remove_node(id_)
        # TODO: handle debug XNs!

        self._execute(graph, all_setup_nodes)

    def executor(self, **kwargs: Any) -> "DAGExecution[P, RVDAG]":
        """Generates a DAGExecution for the DAG.

        Args:
            **kwargs (Any): keyword arguments to be passed to DAGExecution's constructor

        Returns:
            DAGExecution: an executor for the DAG
        """
        return DAGExecution(self, **kwargs)

    def _make_subgraph(
        self,
        target_nodes: Optional[Sequence[Alias]] = None,
        exclude_nodes: Optional[Sequence[Alias]] = None,
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
        if cfg.RUN_DEBUG_NODES:
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

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> RVDAG:
        """Execute the DAG scheduler via a similar interface to the function that describes the dependencies.

        Note: Currently kwargs are not supported.
            They will supported soon!

        Args:
            *args (P.args): arguments to be passed to the call of the DAG
            **kwargs (P.kwargs): keyword arguments to be passed to the call of the DAG

        Returns:
            RVDAG: return value of the DAG's execution

        Raises:
            TawaziUsageError: kwargs are passed
        """
        if kwargs:
            raise TawaziUsageError(f"currently DAG does not support keyword arguments: {kwargs}")
        # 1. generate the subgraph to be executed
        graph = self._make_subgraph()

        # 2. copy the ExecNodes
        call_xn_dict = self._make_call_xn_dict(*args)

        # 3. Execute the scheduler
        all_nodes_dict = self._execute(graph, call_xn_dict)

        # 4. extract the returned value/values
        return self._get_return_values(all_nodes_dict)  # type: ignore[return-value]

    def _make_call_xn_dict(self, *args: Any) -> Dict[Identifier, ExecNode]:
        """Generate the calling ExecNode dict.

        This is a dict containing ExecNodes that will be executed (hence modified) by the DAG scheduler.
        This takes into consideration:
         1. deep copying the ExecNodes
         2. filling the arguments of the call
         3. skipping the copy for setup ExecNodes

        Args:
            *args (Any): arguments to be passed to the call of the DAG

        Returns:
            Dict[Identifier, ExecNode]: The modified ExecNode dict which will be executed by the DAG scheduler.

        Raises:
            TypeError: If called with an invalid number of arguments
        """
        # 1. deepcopy the node_dict because it will be modified by the DAG's execution
        call_xn_dict = copy_non_setup_xns(self.node_dict)

        # 2. parse the input arguments of the pipeline
        # 2.1 default valued arguments can be skipped and not provided!
        # note: if not enough arguments are provided then the code will fail
        # inside the DAG's execution through the raise_err lambda
        if args:
            # 2.2 can't provide more than enough arguments
            if len(args) > len(self.input_uxns):
                raise TypeError(
                    f"The DAG takes a maximum of {len(self.input_uxns)} arguments. {len(args)} arguments provided"
                )

            # 2.3 modify ExecNodes corresponding to input ArgExecNodes
            for ind_arg, arg in enumerate(args):
                node_id = self.input_uxns[ind_arg].id

                call_xn_dict[node_id].result = arg

        return call_xn_dict

    def _get_return_values(self, xn_dict: Dict[Identifier, ExecNode]) -> RVTypes:
        """Extract the return value/values from the output of the DAG's scheduler!

        Args:
            xn_dict (Dict[Identifier, ExecNode]): Modified ExecNodes returned by the DAG's scheduler

        Raises:
            TawaziTypeError: if the type of the return value is not compatible with RVTypes

        Returns:
            RVTypes: the actual values extracted from xn_dict
        """
        return_uxns = self.return_uxns
        if return_uxns is None:
            return None
        if isinstance(return_uxns, UsageExecNode):
            return return_uxns.result(xn_dict)
        if isinstance(return_uxns, (tuple, list)):
            gen = (ren_uxn.result(xn_dict) for ren_uxn in return_uxns)
            if isinstance(return_uxns, tuple):
                return tuple(gen)
            if isinstance(return_uxns, list):
                return list(gen)
        if isinstance(return_uxns, dict):
            return {key: ren_uxn.result(xn_dict) for key, ren_uxn in return_uxns.items()}

        raise TawaziTypeError("Return type for the DAG can only be a single value, Tuple or List")

    # NOTE: this function should be used in case there was a bizarre behavior noticed during
    #   the execution of the DAG via DAG.execute(...)
    def _safe_execute(
        self,
        *args: Any,
        target_nodes: Optional[List[Alias]] = None,
        exclude_nodes: Optional[List[Alias]] = None,
    ) -> Any:
        """Execute the ExecNodes in topological order without priority in for loop manner for debugging purposes (Experimental).

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
            call_xn_dict[xn_id]._execute(call_xn_dict)

        # 4. make returned values
        return self._get_return_values(call_xn_dict)

    def config_from_dict(self, config: Dict[str, Any]) -> None:
        """Allows reconfiguring the parameters of the nodes from a dictionary.

        Args:
            config (Dict[str, Any]): the dictionary containing the config
                example: {"nodes": {"a": {"priority": 3, "is_sequential": True}}, "max_concurrency": 3}

        Raises:
            ValueError: if two nodes are configured by the provided config (which is ambiguous)
        """

        def _override_node_config(n: ExecNode, cfg: Dict[str, Any]) -> bool:
            if "is_sequential" in cfg:
                n.is_sequential = cfg["is_sequential"]

            if "priority" in cfg:
                n.priority = cfg["priority"]
                return True

            return False

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
            self._assign_compound_priority()

    def config_from_yaml(self, config_path: str) -> None:
        """Allows reconfiguring the parameters of the nodes from a YAML file.

        Args:
            config_path: the path to the YAML file
        """
        with open(config_path) as f:
            yaml_config = yaml.load(f, Loader=_UniqueKeyLoader)  # noqa: S506

        self.config_from_dict(yaml_config)

    def config_from_json(self, config_path: str) -> None:
        """Allows reconfiguring the parameters of the nodes from a JSON file.

        Args:
            config_path: the path to the JSON file
        """
        with open(config_path) as f:
            json_config = json.load(f)

        self.config_from_dict(json_config)


# TODO: check if the arguments are the same, then run the DAG using the from_cache.
#  If the arguments are not the same, then rerun the DAG!
class DAGExecution(Generic[P, RVDAG]):
    """A disposable callable instance of a DAG.

    It holds information about the last execution. Hence it is not threadsafe.
    It might be reusable, however it is not recommended to reuse an instance of DAGExecutor!.
    """

    def __init__(
        self,
        dag: DAG[P, RVDAG],
        *,
        target_nodes: Optional[Sequence[Alias]] = None,
        exclude_nodes: Optional[Sequence[Alias]] = None,
        cache_deps_of: Optional[Sequence[Alias]] = None,
        cache_in: str = "",
        from_cache: str = "",
        call_id: Optional[str] = None,
    ):
        """Constructor.

        Args:
            dag (DAG): The attached DAG.
            target_nodes (Optional[List[Alias]]): The leave ExecNodes to execute.
                If None will execute all ExecNodes.
                Defaults to None.
            exclude_nodes (Optional[List[Alias]]): The leave ExecNodes to exclude.
                If None will exclude all ExecNodes.
                Defaults to None.
            cache_deps_of (Optional[List[Alias]]): cache all the dependencies of these nodes.
                This option can not be used together with target_nodes nor exclude_nodes.
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
        self.cache_deps_of = cache_deps_of
        self.cache_in = cache_in
        self.from_cache = from_cache
        # NOTE: from_cache is orthogonal to cache_in which means that if cache_in is set at the same time as from_cache.
        #  in this case the DAG will be loaded from_cache and the results will be saved again to the cache_in file.
        self.call_id = call_id

        # get the leaves ids to execute in case of a subgraph
        self.target_nodes = target_nodes
        self.exclude_nodes = exclude_nodes

        self.xn_dict: Dict[Identifier, ExecNode] = {}
        self.results: Dict[Identifier, Any] = {}

        self._construct_dynamic_attributes()

        self.executed = False

    def _construct_dynamic_attributes(self) -> None:
        self.graph = self._make_graph()
        self.scheduled_nodes = self.graph.nodes

    def _make_graph(self) -> nx.DiGraph:
        """Make the graph of the execution.

        This method is called only once per instance.
        """
        # logic parts
        if self.cache_deps_of is not None:
            return self.dag._make_subgraph(self.cache_deps_of)

        return self.dag._make_subgraph(self.target_nodes, self.exclude_nodes)

    @property
    def cache_in(self) -> str:
        """The path to the file where the execution should be cached.

        Returns:
            str: The path to the file where the execution should be cached.
        """
        return self._cache_in

    @cache_in.setter
    def cache_in(self, cache_in: str) -> None:
        if cache_in and not cache_in.endswith(".pkl"):
            raise ValueError("cache_in should end with.pkl")
        self._cache_in = cache_in

    @property
    def from_cache(self) -> str:
        """Get the file path from which the cached execution should be loaded.

        Returns:
            str: the file path of the cached execution
        """
        return self._from_cache

    @from_cache.setter
    def from_cache(self, from_cache: str) -> None:
        if from_cache and not from_cache.endswith(".pkl"):
            raise ValueError("from_cache should end with.pkl")
        self._from_cache = from_cache

    @property
    def cache_deps_of(self) -> Optional[Sequence[Alias]]:
        """Cache all the dependencies of these nodes.

        Returns:
            Optional[List[Alias]]: List of Aliases passed to cache_deps_of while instantiating DAGExecution
        """
        return self._cache_deps_of

    @cache_deps_of.setter
    def cache_deps_of(self, cache_deps_of: Optional[Sequence[Alias]]) -> None:
        if (
            self.target_nodes is not None or self.exclude_nodes is not None
        ) and cache_deps_of is not None:
            raise ValueError(
                "cache_deps_of can not be used together with target_nodes or exclude_nodes"
            )
        self._cache_deps_of = cache_deps_of

    # we need to reimplement the public methods of DAG here in order to have a constant public interface
    # getters
    def get_nodes_by_tag(self, tag: Tag) -> List[ExecNode]:
        """Get all the nodes with the given tag.

        Args:
            tag (Tag): tag of ExecNodes in question

        Returns:
            List[ExecNode]: corresponding ExecNodes
        """
        if self.executed:
            return [ex_n for ex_n in self.xn_dict.values() if ex_n.tag == tag]
        return self.dag.get_nodes_by_tag(tag)

    def get_node_by_id(self, id_: Identifier) -> ExecNode:
        """Get node with the given id.

        Args:
            id_ (Identifier): id of the ExecNode

        Returns:
            ExecNode: Corresponding ExecNode
        """
        # TODO: ? catch the keyError and
        #   help the user know the id of the ExecNode by pointing to documentation!?
        if self.executed:
            return self.xn_dict[id_]
        return self.dag.get_node_by_id(id_)

    def setup(self) -> None:
        """Does the same thing as DAG.setup. However the `target_nodes` and `exclude_nodes` are taken from the DAGExecution's initization."""
        # TODO: handle the case where cache_deps_of is provided instead of target_nodes and exclude_nodes
        #  in which case the deps_of might have a setup node themselves which should not run.
        #  This is an edge case though that is not important to handle at the current moment.
        self.dag.setup(target_nodes=self.target_nodes, exclude_nodes=self.exclude_nodes)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> RVDAG:
        """Call the DAG.

        Args:
            *args: positional arguments to pass in to the DAG
            **kwargs: keyword arguments to pass in to the DAG

        Raises:
            TawaziUsageError: if the DAGExecution has already been executed.

        Returns:
            RVDAG: the return value of the DAG's Execution
        """
        if self.executed:
            warnings.warn("DAGExecution object's reuse is not recommended.", stacklevel=2)
            self._construct_dynamic_attributes()

        # NOTE: *args will be ignored if self.from_cache is set!
        dag = self.dag

        # maybe call_id will be changed to Union[int, str].
        # Keep call_id as Optional[str] for now
        call_id = self.call_id if self.call_id is not None else ""

        # 1. copy the ExecNodes
        call_xn_dict = dag._make_call_xn_dict(*args)
        if self.from_cache:
            with open(self.from_cache, "rb") as f:
                cached_results = pickle.load(f)  # noqa: S301
            # set the result for the ExecNode that were previously executed
            # this will make them skip execution inside the scheduler
            for id_, result in cached_results.items():
                call_xn_dict[id_].result = result

        # 2. Execute the scheduler
        self.xn_dict = dag._execute(self.graph, call_xn_dict, call_id)
        self.results = {xn.id: xn.result for xn in self.xn_dict.values()}

        # 3. cache in the graph results
        if self.cache_in:
            Path(self.cache_in).parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_in, "wb") as f:
                # NOTE: we are currently only storing the results of the execution,
                #  this means that the configuration of the ExecNodes are lost!
                #  But this is ok since it should not change between executions!
                #  for example, a setup ExecNode should stay a setup ExecNode between caching in the results and reading back the cached results
                #  the same goes for the DAG itself, the behavior when an error is encountered & its concurrency will be controlled via the constructor

                if self.cache_deps_of is not None:
                    non_cacheable_ids: Set[Identifier] = set()
                    for aliases in self.cache_deps_of:
                        ids = self.dag._alias_to_ids(aliases)
                        non_cacheable_ids = non_cacheable_ids.union(ids)

                    to_cache_results = {
                        id_: res
                        for id_, res in self.results.items()
                        if id_ not in non_cacheable_ids
                    }
                else:
                    to_cache_results = self.results
                pickle.dump(
                    to_cache_results, f, protocol=pickle.HIGHEST_PROTOCOL, fix_imports=False
                )

        # TODO: make DAGExecution reusable but do not guarantee ThreadSafety!
        self.executed = True
        # 3. extract the returned value/values
        return dag._get_return_values(self.xn_dict)  # type: ignore[return-value]

    # TODO: add execution order (the order in which the nodes were executed)
