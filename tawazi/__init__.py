"""tawazi is a package that allows parallel execution of a set of functions written in Python."""

# exposing useful objects / Classes
from ._config import cfg
from ._dag import DAG, DAGExecution
from ._decorators import dag, xn
from ._errors import ErrorStrategy
from ._object_helpers import and_, not_, or_

__version__ = "0.3.0a0"

__all__ = ["DAG", "DAGExecution", "xn", "dag", "cfg", "ErrorStrategy", "and_", "or_", "not_"]
