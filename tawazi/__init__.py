from .node import ExecNode  # the rest child classes are hidden from the outside
from .dag import DAG
from .ops import op, to_dag
from .errors import ErrorStrategy
from .config import Cfg

"""
tawazi is a package that allows parallel execution of a set of functions written in Python
isort:skip_file
"""
__version__ = "0.1.2"

__all__ = ["ExecNode", "DAG", "op", "to_dag", "ErrorStrategy", "Cfg"]
