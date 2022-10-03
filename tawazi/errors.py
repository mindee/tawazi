from enum import Enum, unique


class DAGBaseException(BaseException):
    pass


@unique
class ErrorStrategy(str, Enum):
    # supported behavior following a raised error
    strict: str = "strict"
    all_children: str = "all-children"
    permissive: str = "permissive"
