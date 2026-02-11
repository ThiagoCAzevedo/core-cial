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
        self.log.info("Inicializando CleanerBase")

        try:
            self.os_user = os.getenv("USERNAME")
            self.log.info(f"Usuário do sistema obtido: {self.os_user}")
        except Exception:
            self.log.error("Erro ao obter variável USERNAME", exc_info=True)
            raise

    def _resolve_path(self, path_template):
        self.log.info("Resolvendo path com template")

        try:
            path = Path(path_template.format(username=self.os_user)).resolve()
            self.log.info(f"Path resolvido: {path}")
            return path

        except Exception:
            self.log.error("Erro ao resolver path do template", exc_info=True)
            raise

    def _get_path(self, env_key: str) -> Path:
        self.log.info(f"Buscando path a partir da env '{env_key}'")
        try:
            env_value = os.getenv(env_key)
            if not env_value:
                self.log.error(f"Variável de ambiente '{env_key}' não encontrada")
                raise ValueError(f"ENV {env_key} não definida")

            path = Path(self._resolve_path(env_value))
            self.log.info(f"Path final obtido: {path}")
            return path

        except Exception:
            self.log.error("Erro ao montar path a partir da variável de ambiente", exc_info=True)
            raise

    def _load_file(self, env_key, rows_to_skip=0, separator="\t"):
        self.log.info(f"Carregando arquivo definido na env '{env_key}'")

        try:
            path = self._get_path(env_key)
            data_map = DataLoader(path).load_data(rows_to_skip, separator)

            if path not in data_map:
                self.log.error(f"Arquivo {path} não encontrado no data_map retornado")
                raise FileNotFoundError(str(path))

            self.log.info(f"Arquivo carregado com sucesso: {path}")
            return data_map[path]

        except Exception:
            self.log.error("Erro ao carregar arquivo", exc_info=True)
            raise

    def _rename(self, df: pl.DataFrame, rename_map: dict) -> pl.DataFrame:
        self.log.info(f"Renomeando colunas: {list(rename_map.keys())}")

        try:
            df_renamed = df.select(list(rename_map.keys())).rename(rename_map)
            self.log.info("Renomeação concluída com sucesso")
            return df_renamed

        except Exception:
            self.log.error("Erro ao renomear colunas", exc_info=True)
            raise