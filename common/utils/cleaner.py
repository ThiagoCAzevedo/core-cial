from __future__ import annotations
from typing import Any, Dict


def remove_none_values(data: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in data.items() if v is not None}


def strip_strings(data: Dict[str, Any]) -> Dict[str, Any]:
    new = {}
    for k, v in data.items():
        if isinstance(v, str):
            new[k] = v.strip()
        else:
            new[k] = v
    return new