from .loader import DataLoader
from pathlib import Path
from dotenv import load_dotenv
from helpers.log.logger import logger
import polars as pl
import os


load_dotenv("config/.env")


class CleanerBase:
    def __init__(self):
        self.log = logger("data_helpers")
        self.log.info("Initializing CleanerBase")

        try:
            self.os_user = os.getenv("USERNAME")
            self.log.info(f"System user obtained: {self.os_user}")
        except Exception:
            self.log.error("Error retrieving USERNAME environment variable", exc_info=True)
            raise

    def _resolve_path(self, path_template):
        self.log.info("Resolving path from template")

        try:
            path = Path(path_template.format(username=self.os_user)).resolve()
            self.log.info(f"Resolved path: {path}")
            return path

        except Exception:
            self.log.error("Error resolving path from template", exc_info=True)
            raise

    def _get_path(self, env_key: str) -> Path:
        self.log.info(f"Retrieving path from environment key '{env_key}'")

        try:
            env_value = os.getenv(env_key)
            if not env_value:
                self.log.error(f"Environment variable '{env_key}' not found")
                raise ValueError(f"ENV {env_key} is not defined")

            path = Path(self._resolve_path(env_value))
            self.log.info(f"Final resolved path: {path}")
            return path

        except Exception:
            self.log.error("Error retrieving path from environment variable", exc_info=True)
            raise

    def _load_file(self, env_key, rows_to_skip=0, separator="\t"):
        self.log.info(f"Loading file defined in environment key '{env_key}'")

        try:
            path = self._get_path(env_key)
            data_map = DataLoader(path).load_data(rows_to_skip, separator)

            if path not in data_map:
                self.log.error(f"File {path} not found in DataLoader returned map")
                raise FileNotFoundError(str(path))

            self.log.info(f"File loaded successfully: {path}")
            return data_map[path]

        except Exception:
            self.log.error("Error loading file", exc_info=True)
            raise

    def _rename(self, df: pl.DataFrame, rename_map: dict) -> pl.DataFrame:
        self.log.info(f"Renaming columns: {list(rename_map.keys())}")

        try:
            df_renamed = df.select(list(rename_map.keys())).rename(rename_map)
            self.log.info("Column renaming completed successfully")
            return df_renamed

        except Exception:
            self.log.error("Error renaming columns", exc_info=True)
            raise