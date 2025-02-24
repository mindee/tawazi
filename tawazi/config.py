"""configuration parameters for Tawazi."""

import pydantic
from packaging.version import Version

if Version(str(pydantic.VERSION)) < Version("2"):
    # pydantic v1
    from pydantic.env_settings import BaseSettings
else:
    # pydantic v2
    from pydantic.v1.env_settings import BaseSettings

from tawazi.consts import Resource, XNOutsideDAGCall


class Config(BaseSettings):
    """Class to set configuration parameters for Tawazi."""

    # whether the default in tawazi is sequentiality or not.
    # This is helpful to reduce your code size and avoid repeating @xn(is_sequentiality=True/False)
    TAWAZI_IS_SEQUENTIAL: bool = False

    # the execution time of each ExecNode will be profiled.
    # Defaults to False.
    # In the future, Tawazi might include an option to profile specific nodes, hence the variable name :).
    TAWAZI_PROFILE_ALL_NODES: bool = False

    # Weather to run the graphs in debug mode or not
    RUN_DEBUG_NODES: bool = False

    # Behavior when an ExecNode is executed outside of a DAG
    # Defaults to "Warning"
    # Options are:
    # - "Warning": print a warning
    # - "Error": raise an error
    # - "Ignore": do nothing
    TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR: XNOutsideDAGCall = XNOutsideDAGCall.error

    # choose the default Resource to use to execute the ExecNodes
    TAWAZI_DEFAULT_RESOURCE: Resource = Resource.thread


cfg = Config()
