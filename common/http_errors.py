from fastapi import HTTPException
from common.logger import logger


log = logger("http")


def http_400(msg: str, detail: str | Exception | None = None):
    error_detail = f"{msg}: {detail}" if detail else msg
    log.error(f"400 — {error_detail}")
    raise HTTPException(status_code=400, detail=str(error_detail))


def http_404(msg: str, detail: str | Exception | None = None):
    error_detail = f"{msg}: {detail}" if detail else msg
    log.error(f"404 — {error_detail}")
    raise HTTPException(status_code=404, detail=str(error_detail))


def http_500(msg: str, detail: str | Exception | None = None):
    error_detail = f"{msg}: {detail}" if detail else msg
    log.error(f"500 — {error_detail}")
    raise HTTPException(status_code=500, detail=str(error_detail))


def http_502(msg: str, detail: str | Exception | None = None):
    error_detail = f"{msg}: {detail}" if detail else msg
    log.error(f"502 — {error_detail}")
    raise HTTPException(status_code=502, detail=str(error_detail))