from __future__ import annotations
from typing import Any
import json
import polars as pl
from pathlib import Path


def load_json(path: str | Path) -> Any:
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_excel(path: str | Path) -> pl.DataFrame:
    path = Path(path)
    return pl.read_excel(path)