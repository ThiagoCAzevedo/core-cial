from dataclasses import dataclass


@dataclass(frozen=True)
class ForecastRecord:
    partnumber: str
    qty_usage: float
    qty_unit: int