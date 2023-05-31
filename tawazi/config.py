"""configuration parameters for Tawazi."""

from pydantic import BaseSettings, Field, validator

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
    TAWAZI_DEFAULT_RESOURCE = Resource.thread

    # Logger settings
    LOGURU_LEVEL: str = Field("PROD", env="TAWAZI_LOGGER_LEVEL")
    LOGURU_BACKTRACE: bool = Field(False, env="TAWAZI_LOGGER_BT")
    # Caution: to set to False if used in prod (exposes variable names)
    LOGURU_DIAGNOSE: bool = Field(False, env="TAWAZI_LOGGER_DIAGNOSE")

    @validator("LOGURU_LEVEL")
    def _validate_loguru_level(cls, v: str) -> str:  # noqa: N805
        if v == "PROD":
            from loguru import logger

            logger.disable("tawazi")

        return v

    # validator for TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR
    @validator("TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR")
    def _validate_execnode_outside_dag_behavior(cls, v: str) -> str:  # noqa: N805
        accepted_values = XNOutsideDAGCall.__members__.values()
        if v not in accepted_values:
            raise ValueError(
                f"TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR must be one of {accepted_values}"
            )
        return v

    @validator("TAWAZI_DEFAULT_RESOURCE")
    def _validate_default_resource(cls, v: str) -> str:  # noqa: N805
        accepted_values = Resource.__members__.values()
        if v not in accepted_values:
            raise ValueError(f"TAWAZI_DEFAULT_RESOURCE must be one of {accepted_values}")
        return v


cfg = Config()  # type: ignore[call-arg]
