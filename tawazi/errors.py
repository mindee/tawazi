from enum import Enum


class DAGBaseException(BaseException):
    pass


class ErrorStrategy(Enum):
    # supported behavior following a raised error
    strict: str = "strict"
    all_children: str = "all-children"
    permissive: str = "permissive"
