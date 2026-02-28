from __future__ import annotations
from typing import TypedDict


class AssemblyRecordDTO(TypedDict):
    knr: str
    model: str
    lfdnr_sequence: str
    werk: str
    spj: str
    lane: str
    takt: str
    knr_fx4pd: str