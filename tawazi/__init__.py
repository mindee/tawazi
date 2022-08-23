from .dag import ExecNode, DAG
from .ops import op, to_dag
from .errors import ErrorStrategy

"""
parallel-dag is a package that allows parallel execution of a set of functions written in Python
isort:skip_file
"""
__version__ = "0.1.2"

# todo change format to inslcude precise time because here we are dealing with parallel programming
import logging

logging.basicConfig(
    format="%(name)s >>> %(asctime)s %(levelname)s %(threadName)s %(message)s"
)

__all__ = ["ExecNode", "DAG", "op", "to_dag", "ErrorStrategy"]
