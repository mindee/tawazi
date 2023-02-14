# exposing useful objects / Classes
from .dag import DAG, DAGExecution
from .decorators import xn, dag
from .errors import ErrorStrategy
from .config import Cfg

"""
tawazi is a package that allows parallel execution of a set of functions written in Python
isort:skip_file
"""
__version__ = "0.2.0"

__all__ = ["DAG", "DAGExecution", "xn", "dag", "ErrorStrategy", "Cfg"]
