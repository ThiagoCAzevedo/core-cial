from typing import Union, List
from helpers.log.logger import logger
import polars as pl


class DataLoader:
    def __init__(self, file_paths: Union[str, List[str]]):
        self.log = logger("data_helpers")
        self.log.info("Inicializando DataLoader")

        try:
            # Garante que sempre será uma lista
            self.file_paths = file_paths if isinstance(file_paths, list) else [file_paths]
            self.log.info(f"Arquivos recebidos: {self.file_paths}")

        except Exception:
            self.log.error("Erro ao inicializar DataLoader", exc_info=True)
            raise

    def define_ext_file(self, file_path: str) -> str:
        try:
            ext = file_path.suffix
            self.log.info(f"Extensão identificada para '{file_path}': {ext}")
            return ext

        except Exception:
            self.log.error("Erro ao obter extensão do arquivo", exc_info=True)
            raise

    def load_data(self):
        loaded_data = {}
        self.log.info("Iniciando carregamento de dados")

        for file_path in self.file_paths:
            try:
                ext = self.define_ext_file(file_path)
                self.log.info(f"Carregando arquivo '{file_path}' com extensão '{ext}'")

                # Excel
                if ext in [".xlsx", ".xls", ".xlsm", ".XLSX"]:
                    df = pl.read_excel(
                        file_path,
                        raise_if_empty=False,
                        read_csv_options={"infer_schema_length": 10000}
                    )
                    self.log.info(f"Arquivo Excel carregado: {file_path}")

                # Parquet
                elif ext == ".parquet":
                    df = pl.scan_parquet(file_path).collect()
                    self.log.info(f"Arquivo Parquet carregado: {file_path}")

                # CSV / TXT
                elif ext in [".csv", ".txt"]:
                    df = pl.scan_csv(
                        file_path,
                        truncate_ragged_lines=True,
                        encoding="utf8-lossy",
                        has_header=True,
                    ).collect()
                    self.log.info(f"Arquivo CSV/TXT carregado: {file_path}")

                else:
                    self.log.error(f"Extensão não suportada: {ext}")
                    raise ValueError(f"Formato de arquivo não suportado: {ext}")

                loaded_data[file_path] = df

            except Exception:
                self.log.error(f"Erro ao carregar arquivo '{file_path}'", exc_info=True)
                raise

        self.log.info("Carregamento de arquivos concluído com sucesso")
        return loaded_data