from typing import Union, List
from helpers.log.logger import logger
import polars as pl


class DataLoader:
    def __init__(self, file_paths: Union[str, List[str]]):
        self.log = logger("data_helpers")
        self.log.info("Initializing DataLoader")

        try:
            # Ensure it is always a list
            self.file_paths = file_paths if isinstance(file_paths, list) else [file_paths]
            self.log.info(f"Files received: {self.file_paths}")

        except Exception:
            self.log.error("Error initializing DataLoader", exc_info=True)
            raise

    def define_ext_file(self, file_path: str) -> str:
        try:
            ext = file_path.suffix
            self.log.info(f"File extension identified for '{file_path}': {ext}")
            return ext

        except Exception:
            self.log.error("Error retrieving file extension", exc_info=True)
            raise

    def load_data(self, rows_to_skip, separator):
        loaded_data = {}
        self.log.info("Starting data loading process")

        for file_path in self.file_paths:
            try:
                ext = self.define_ext_file(file_path)
                self.log.info(f"Loading file '{file_path}' with extension '{ext}'")

                # Excel
                if ext in [".xlsx", ".xls", ".xlsm", ".XLSX"]:
                    df = pl.read_excel(
                        file_path,
                        raise_if_empty=False,
                        read_csv_options={"infer_schema_length": 10000}
                    )
                    self.log.info(f"Excel file loaded: {file_path}")

                # Parquet
                elif ext == ".parquet":
                    df = pl.scan_parquet(file_path).collect()
                    self.log.info(f"Parquet file loaded: {file_path}")

                # CSV / TXT
                elif ext in [".csv", ".txt"]:
                    df = pl.scan_csv(
                        file_path,
                        truncate_ragged_lines=True,
                        encoding="utf8-lossy",
                        has_header=True,
                        skip_rows=rows_to_skip,
                        separator=separator
                    ).collect()
                    self.log.info(f"CSV/TXT file loaded: {file_path}")

                else:
                    self.log.error(f"Unsupported file extension: {ext}")
                    raise ValueError(f"Unsupported file format: {ext}")

                loaded_data[file_path] = df

            except Exception:
                self.log.error(f"Error loading file '{file_path}'", exc_info=True)
                raise

        self.log.info("All files loaded successfully")
        return loaded_data