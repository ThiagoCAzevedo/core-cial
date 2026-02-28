from __future__ import annotations
from typing import Any, Dict
import polars as pl
from config.settings import settings
from common.logger import logger
from io import StringIO


class CleanerBase:
    def __init__(self):
        self.log = logger("cleaner")


    def _load_file(self, settings_key: str, rows_to_skip: int | None = None, separator: str = "\t") -> pl.DataFrame:
        try:
            file_path = getattr(settings, settings_key)
            self.log.info(f"Loading file from {settings_key}: {file_path}")

            extension = file_path.lower().split(".")[-1]

            if extension in ("txt", "csv", "tsv"):
                if rows_to_skip is None:
                    rows_to_skip = 0

                with open(file_path, "rb") as f:
                    raw = f.read()

                try:
                    text = raw.decode("utf-8")
                    self.log.info("File decoded as UTF‑8")
                except UnicodeDecodeError:
                    text = raw.decode("latin1")
                    self.log.info("File decoded as LATIN‑1 (fallback)")

                df = pl.read_csv(
                    StringIO(text),
                    skip_rows=rows_to_skip,
                    separator=separator,
                    truncate_ragged_lines=True, 
                    infer_schema_length=5000     
                )

                self.log.info(
                    f"Delimited file loaded: {df.shape[0]} rows, {df.shape[1]} columns (skip_rows={rows_to_skip})"
                )

            elif extension in ("xlsx", "xls"):
                if rows_to_skip is not None:
                    raise ValueError(
                        f"skip_rows is only allowed for TXT/CSV/TSV files. {extension} does not support skip_rows."
                    )

                df = pl.read_excel(file_path)
                self.log.info(f"Excel file loaded: {df.shape[0]} rows, {df.shape[1]} columns")

            else:
                raise ValueError(f"Unsupported file type: .{extension}")

            return df

        except Exception as e:
            self.log.error(f"Error loading file from {settings_key}: {str(e)}", exc_info=True)
            raise

    def _rename(
        self, 
        df: pl.DataFrame, 
        rename_map: Dict[str, str]
    ) -> pl.DataFrame:
        try:
            df = df.select(list(rename_map.keys())).rename(rename_map)
            self.log.info(f"Columns renamed successfully: {list(rename_map.values())}")
            return df
        except Exception as e:
            self.log.error(f"Error renaming columns", exc_info=True)
            raise
