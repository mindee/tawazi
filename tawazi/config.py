from pydantic import BaseSettings


class Config(BaseSettings):
    TAWAZI_IS_SEQUENTIAL: bool = False

    # Logger settings
    LOGURU_LEVEL: str = "ERROR"
    LOGURU_BACKTRACE: bool = True
    # Caution: to set to False if used in prod (exposes variable names)
    LOGURU_DIAGNOSE: bool = False


Cfg = Config()
if Cfg.LOGURU_LEVEL == "PROD":
    from loguru import logger

    logger.disable("tawazi")
