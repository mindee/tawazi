from pydantic import BaseSettings


class Config(BaseSettings):
    TAWAZI_IS_SEQUENTIAL: bool = True

    # Logger settings
    LOGURU_LEVEL: str = "INFO"
    LOGURU_BACKTRACE: bool = True
    # Caution: to set to False if used in prod (exposes variable names)
    LOGURU_DIAGNOSE: bool = False


Cfg = Config()
