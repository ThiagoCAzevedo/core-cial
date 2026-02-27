from dataclasses import dataclass


@dataclass(frozen=True)
class ConsumptionRecord:
    partnumber: str
    lb_balance: float