from pydantic import BaseSettings


class Config(BaseSettings):
    # Tawazi configuration
    TAWAZI_IS_SEQUENTIAL: bool = False
    # Weather to run the graphs in debug mode or not
    RUN_DEBUG_NODES: bool = False

    # Logger settings
    LOGURU_LEVEL: str = "PROD"
    LOGURU_BACKTRACE: bool = True
    # Caution: to set to False if used in prod (exposes variable names)
    LOGURU_DIAGNOSE: bool = False


Cfg = Config()
if Cfg.LOGURU_LEVEL == "PROD":
    from loguru import logger

    logger.disable("tawazi")
