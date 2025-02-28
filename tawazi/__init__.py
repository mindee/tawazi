"""tawazi is a package that allows parallel execution of a set of functions written in Python."""

# exposing useful objects / Classes
from ._dag import DAG, AsyncDAG, AsyncDAGExecution, DAGExecution
from ._decorators import dag, xn
from ._object_helpers import and_, not_, or_
from .config import cfg
from .consts import Resource

__version__ = "0.6.1"

__all__ = [
    "AsyncDAG",
    "DAG",
    "AsyncDAGExecution",
    "DAGExecution",
    "xn",
    "dag",
    "cfg",
    "and_",
    "or_",
    "not_",
    "Resource",
]
