from .node import ExecNode  # the rest child classes are hidden from the outside
from .dag import DAG
from .ops import op, to_dag
from .errors import ErrorStrategy

"""
tawazi is a package that allows parallel execution of a set of functions written in Python
isort:skip_file
"""
__version__ = "0.1.2"

# todo change format to include precise time because here we are dealing with parallel programming
import logging

logging.basicConfig(format="%(name)s >>> %(asctime)s %(levelname)s %(threadName)s %(message)s")

__all__ = ["ExecNode", "DAG", "op", "to_dag", "ErrorStrategy"]
