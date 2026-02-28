# core/modules/assembly/infrastructure/processor.py
from __future__ import annotations
from typing import Dict, List, Any
import polars as pl

from common.logger import logger


class AssemblyLazyExtractor:
    def __init__(self, response: Dict[str, Any]):
        self.log = logger("assembly")
        self.response = response

    def extract(self) -> pl.LazyFrame:
        self.log.info("Extracting infos from JSON")
        rows: List[Dict[str, Any]] = []

        try:
            for lane, lane_data in self.response.items():
                if not (lane.startswith("lane_") or lane == "reception"):
                    continue

                for _, fb in lane_data.items():
                    for _, tact in fb.items():
                        if not isinstance(tact, dict):
                            continue

                        car = tact.get("CAR")
                        if not car:
                            continue

                        rows.append({
                            "knr": car.get("KNR"),
                            "model": car.get("MODELL"),
                            "lfdnr_sequence": car.get("LFDNR"),
                            "werk": car.get("WERK"),
                            "spj": car.get("SPJ"),
                            "lane": tact.get("LANE", lane),
                            "takt": tact.get("TACT"),
                        })

            lf = pl.LazyFrame(rows)
            self.log.info(f"JSON Registers: {lf.select(pl.len()).collect().item()}")
            return lf

        except Exception as exc:
            self.log.error("Failed to extract infos from JSON: ", exc_info=True)
            raise exc


class AssemblyLazyTransformer:
    def __init__(self, lf: pl.LazyFrame):
        self.lf = lf
        self.log = logger("assembly")

    def transform(self) -> pl.LazyFrame:
        self.log.info("Initialized transformations")

        try:
            return (
                self.lf
                .with_columns([
                    pl.col("lane").str.replace("lane_", ""),
                    pl.col("lfdnr_sequence").cast(pl.Utf8)
                ])
            )
        except Exception as exc:
            self.log.error("Error during transform()", exc_info=True)
            raise exc

    def attach_fx4pd(self) -> pl.LazyFrame:
        self.log.info("Creating column knr_fx4pd")

        try:
            return self.lf.with_columns(
                (pl.col("werk") + pl.col("spj") + pl.col("knr")).alias("knr_fx4pd")
            )
        except Exception as exc:
            self.log.error("Error creating knr_fx4pd", exc_info=True)
            raise exc