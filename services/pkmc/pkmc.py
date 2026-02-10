from helpers.data.cleaner import CleanerBase
from helpers.log.logger import logger
import polars as pl


class PKMC_DefineDataframe(CleanerBase):
    def __init__(self):
        self.log = logger("static")
        self.log.info("Inicializando PKMC_DefineDataframe")

        CleanerBase.__init__(self)

    def create_df(self):
        self.log.info("Carregando arquivo do PKMC_PATH como LazyFrame")

        try:
            df = self._load_file("PKMC_PATH").lazy()
            self.log.info("LazyFrame criado com sucesso a partir do PKMC_PATH")
            return df

        except Exception:
            self.log.error("Erro ao carregar arquivo PKMC_PATH", exc_info=True)
            raise
    

class PKMC_Cleaner(CleanerBase):
    def __init__(self):
        self.log = logger("static")
        self.log.info("Inicializando PKMC_Cleaner")
        
        CleanerBase.__init__(self)
        
    def filter_columns(self, df):
        self.log.info("Filtrando colunas: deposit_type == 'B01'")

        try:
            df = df.filter(pl.col("deposit_type") == "B01")
            self.log.info("Filtro aplicado com sucesso")
            return df

        except Exception:
            self.log.error("Erro ao filtrar colunas em PKMC", exc_info=True)
            raise
    
    def clean_columns(self, df):
        self.log.info("Iniciando limpeza das colunas (qty_max_box, partnumber)")
        
        try:
            df = df.with_columns(
                pl.col("qty_max_box")
                    .cast(pl.Utf8)
                    .str.replace_all(r"(?i)max", "")
                    .str.replace_all(r"[ :]", "")
                    .str.replace_all(r"\D+", "")
                    .cast(pl.Int64, strict=False)
                    .fill_null(0),

                pl.col("partnumber")
                    .cast(pl.Utf8)
                    .str.strip()         
                    .str.replace_all(r"\s+", "")
                    .str.replace_all(r"\.", "")
                    .str.replace_all(r"[^\w-]", "")
                    .str.to_uppercase()
            )

            self.log.info("Colunas limpas com sucesso")
            return df

        except Exception:
            self.log.error("Erro ao limpar colunas em PKMC", exc_info=True)
            raise

    def create_columns(self, df):
        self.log.info("Criando colunas: total_theoretical_qty, qty_for_restock, lb_balance, rack")

        try:
            df = df.with_columns([
                (pl.col("qty_per_box") * pl.col("qty_max_box")).alias("total_theoretical_qty"),
                (pl.col("qty_per_box") * (pl.col("qty_max_box") - 1)).alias("qty_for_restock"),
                pl.lit(2000).alias("lb_balance"),
                pl.col("supply_area").str.extract(r"(P\d+[A-Z]?)", 0).alias("rack")
            ])

            df = df.with_columns([
                (pl.col("lb_balance") / (pl.col("qty_per_box") - 1))
                    .round(2)
                    .alias("lb_balance_box")
            ])

            df = df.drop_nulls("rack")
            df = df.with_row_index(name="id")

            self.log.info("Colunas criadas e dataframe atualizado com sucesso")
            return df

        except Exception:
            self.log.error("Erro ao criar colunas em PKMC", exc_info=True)
            raise

    def rename_columns(self, df):
        self.log.info("Renomeando colunas conforme dicionário padrão PKMC")
        
        rename_map = {
            "Material": "partnumber",
            "Área abastec.prod.": "supply_area",
            "Nº circ.regul.": "num_reg_circ",
            "Tipo de depósito": "deposit_type",
            "Posição no depósito": "deposit_position",
            "Container": "container",
            "Texto breve de material": "description",
            "Norma de embalagem": "pack_standard",
            "Quantidade Kanban": "qty_per_box", 
            "Posição de armazenamento": "qty_max_box",
        }

        try:
            df = self._rename(df, rename_map)
            self.log.info("Renomeação concluída com sucesso")
            return df

        except Exception:
            self.log.error("Erro ao renomear colunas em PKMC", exc_info=True)
            raise