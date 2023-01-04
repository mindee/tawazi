from pydantic import BaseSettings, Field


class Config(BaseSettings):
    TAWAZI_IS_SEQUENTIAL: bool = Field(False, env="TAWAZI_IS_SEQUENTIAL")
    # Weather to run the graphs in debug mode or not
    RUN_DEBUG_NODES: bool = False
    # Logger settings
    LOGURU_LEVEL: str = Field("CRITICAL", env="TAWAZI_LOGGER_LEVEL")
    LOGURU_BACKTRACE: bool = Field(False, env="TAWAZI_LOGGER_BT")
    # Caution: to set to False if used in prod (exposes variable names)
    LOGURU_DIAGNOSE: bool = Field(False, env="TAWAZI_LOGGER_DIAGNOSE")


Cfg = Config()
if Cfg.LOGURU_LEVEL == "PROD":
    from loguru import logger

    logger.disable("tawazi")
