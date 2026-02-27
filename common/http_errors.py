from fastapi import HTTPException
from common.logger import logger


log = logger("http")


def http_400(msg: str, exc: Exception | None = None):
    log.error(f"400 — {msg}", exc_info=bool(exc))
    raise HTTPException(
        status_code=400,
        detail=f"{msg}: {exc}" if exc else msg,
    )


def http_404(msg: str, exc: Exception | None = None):
    log.error(f"404 — {msg}", exc_info=bool(exc))
    raise HTTPException(
        status_code=404,
        detail=f"{msg}: {exc}" if exc else msg,
    )


def http_500(msg: str, exc: Exception | None = None):
    log.error(f"500 — {msg}", exc_info=bool(exc))
    raise HTTPException(
        status_code=500,
        detail=f"{msg}: {exc}" if exc else msg,
    )


def http_502(msg: str, exc: Exception | None = None):
    log.error(f"502 — {msg}", exc_info=bool(exc))
    raise HTTPException(
        status_code=502,
        detail=f"{msg}: {exc}" if exc else msg,
    )