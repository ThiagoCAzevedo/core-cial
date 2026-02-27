from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class AssemblyRecord:
    knr: str
    model: str
    lfdnr_sequence: str
    werk: str
    spj: str
    lane: str
    takt: str
    knr_fx4pd: str