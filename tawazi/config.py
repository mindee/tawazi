"""configuration parameters for Tawazi."""

from pydantic import BaseSettings, Field


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

    # Logger settings
    LOGURU_LEVEL: str = Field("PROD", env="TAWAZI_LOGGER_LEVEL")
    LOGURU_BACKTRACE: bool = Field(False, env="TAWAZI_LOGGER_BT")
    # Caution: to set to False if used in prod (exposes variable names)
    LOGURU_DIAGNOSE: bool = Field(False, env="TAWAZI_LOGGER_DIAGNOSE")


Cfg = Config()
if Cfg.LOGURU_LEVEL == "PROD":
    from loguru import logger

    logger.disable("tawazi")
