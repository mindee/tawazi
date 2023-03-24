"""tawazi is a package that allows parallel execution of a set of functions written in Python."""

# exposing useful objects / Classes
from .config import Cfg
from .dag import DAG, DAGExecution
from .decorators import dag, xn
from .errors import ErrorStrategy
from .object_helpers import and_, not_, or_

__version__ = "0.2.0"

__all__ = ["DAG", "DAGExecution", "xn", "dag", "ErrorStrategy", "Cfg", "and_", "or_", "not_"]
