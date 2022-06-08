from enum import Enum

class DAGBaseException(BaseException):
    pass

class ErrorStrategy(Enum):
    # supported behavior following a raised error
    strict = "strict"
    all_children = "all-children"
    permissive = "premissive"
