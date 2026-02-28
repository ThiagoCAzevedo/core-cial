from __future__ import annotations
from pathlib import Path
from common.logger import logger
from config.settings import settings
import polars as pl


class ForecastLoaders:
    def __init__(self):
        self.log = logger("forecast")

    def _load_excel_lazy(self, file_path: str | Path) -> pl.LazyFrame:
        file_path = Path(file_path)

        if not file_path.exists():
            msg = f"File not found: {file_path}"
            self.log.error(msg)
            raise FileNotFoundError(msg)

        try:
            self.log.info(f"Loading Excel file: {file_path}")
            df = pl.read_excel(file_path)
            self.log.info(f"File loaded successfully: {file_path}")
            return df.lazy()

        except Exception as exc:
            self.log.error(f"Failed to load Excel file: {file_path}", exc_info=True)
            raise exc

    def load_fx4pd(self) -> pl.LazyFrame:
        path = settings.FX4PD_PATH
        self.log.info(f"Loading FX4PD file from path: {path}")
        return self._load_excel_lazy(path)