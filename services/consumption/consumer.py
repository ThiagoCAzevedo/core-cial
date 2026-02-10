from sqlalchemy import select
from database.models.forecast import Forecast
from database.models.assembly import Assembly
from database.models.pkmc import PKMC
from database.queries import SelectInfos, UpdateInfos
from helpers.log.logger import logger
import polars as pl
    

class ConsumeValues(SelectInfos):
    def __init__(self, db):
        self.log = logger("consumption")
        self.log.info("Inicializando ConsumeValues")
        
        SelectInfos.__init__(self, db)

    def values_to_consume(self):
        self.log.info("Montando query para obter valores de consumo (Forecast x Assembly x PKMC)")
        
        try:
            stmt = (
                select(
                    Forecast.partnumber,
                    Forecast.takt,
                    Forecast.rack,
                    Forecast.knr_fx4pd,
                    Forecast.qty_usage,
                    Assembly.takt.label("assembly_takt"),
                    PKMC.partnumber.label("pkmc_partnumber"),
                    PKMC.lb_balance,
                )
                .join(
                    Assembly,
                    (Forecast.knr_fx4pd == Assembly.knr_fx4pd)
                    & (Forecast.takt == Assembly.takt)
                )
                .join(
                    PKMC,
                    PKMC.partnumber == Forecast.partnumber
                )
            )
            self.log.info("Query SQL montada com sucesso")

        except Exception as e:
            self.log.error("Erro ao montar query SQL", exc_info=True)
            raise

        try:
            df = self.select(stmt)
            self.log.info(f"Select concluído. Registros retornados: {df.height()}")

        except Exception as e:
            self.log.error("Erro ao executar SELECT no banco", exc_info=True)
            raise

        try:
            self.log.info("Calculando lb_balance atualizado baseado em qty_usage")
            df = (
                df.with_columns(
                    (pl.col("lb_balance") - pl.col("qty_usage").fill_null(0))
                    .alias("lb_balance")
                )
                .select(["partnumber", "lb_balance"])
                .collect()
            )
            self.log.info("Cálculo concluído e dataframe final montado")

        except Exception as e:
            self.log.error("Erro ao calcular lb_balance no dataframe", exc_info=True)
            raise

        return df

    def _update_infos(self, df, batch_size):
        self.log.info(f"Iniciando update na tabela PKMC para {df.height()} registros")

        try:
            update = UpdateInfos(self.db)

            update.update_df(
                table_name="pkmc",
                df=df,
                key_column="partnumber",
                batch_size=batch_size
            )

            self.log.info("Update finalizado com sucesso")

        except Exception as e:
            self.log.error("Erro ao executar update no banco", exc_info=True)
            raise