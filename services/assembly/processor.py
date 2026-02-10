import polars as pl
from helpers.log.logger import logger


class DefineDataFrame:
    def __init__(self, response: dict):
        self.log = logger("assembly")
        self.log.info("Inicializando DefineDataFrame")

        try:
            self.response = response
            self.log.info("Response recebido para processamento")
        except Exception:
            self.log.error("Erro ao inicializar response em DefineDataFrame", exc_info=True)
            raise

    def extract_car_records(self, cleaned):
        self.log.info("Iniciando extração de registros CAR")

        try:
            registers = []

            for lane_key, lane_val in cleaned.items():
                if lane_key.startswith("lane_") or lane_key.startswith("reception"):
                    for fb_key, fb_val in lane_val.items():
                        for tact_key, tact_val in fb_val.items():
                            if isinstance(tact_val, dict) and "CAR" in tact_val and tact_val["CAR"]:
                                car = tact_val["CAR"]

                                registers.append({
                                    "knr": car.get("KNR"),
                                    "model": car.get("MODELL"),
                                    "lfdnr_sequence": car.get("LFDNR"),
                                    "werk": car.get("WERK"),
                                    "spj": car.get("SPJ"),
                                    "lane": tact_val.get("LANE", lane_key),
                                    "takt": tact_val.get("TACT"),
                                })

            df = pl.DataFrame(registers)

            self.log.info(f"Total de registros CAR extraídos: {df.height()}")

            return df

        except Exception:
            self.log.error("Erro ao extrair registros CAR do JSON limpo", exc_info=True)
            raise
    


class TransformDataFrame:
    def __init__(self, df):
        self.log = logger("assembly")
        self.log.info("Inicializando TransformDataFrame")

        try:
            self.df = df
            self.log.info(f"DataFrame recebido — linhas: {df.height()}, colunas: {len(df.columns)}")
        except Exception:
            self.log.error("Erro ao inicializar DataFrame em TransformDataFrame", exc_info=True)
            raise

    def transform(self):
        self.log.info("Aplicando transformações (replace lane_, casting lfdnr_sequence)")

        try:
            df = (
                self.df
                .with_columns([
                    pl.col("lane").str.replace("lane_", ""),
                    pl.col("lfdnr_sequence").cast(pl.Utf8)
                ])
            )

            self.log.info("Transformação concluída com sucesso")
            return df

        except Exception:
            self.log.error("Erro ao transformar DataFrame em TransformDataFrame.transform()", exc_info=True)
            raise
    
    def attach_fx4pd(self):
        self.log.info("Criando coluna knr_fx4pd")

        try:
            df = self.df.with_columns(
                (pl.col("werk") + pl.col("spj") + pl.col("knr")).alias("knr_fx4pd")
            )

            self.log.info("Coluna knr_fx4pd criada com sucesso")
            return df

        except Exception:
            self.log.error("Erro ao criar coluna knr_fx4pd em TransformDataFrame.attach_fx4pd()", exc_info=True)
            raise