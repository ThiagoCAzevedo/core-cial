from database.queries import SelectInfos
from sqlalchemy import select
from database.models.assembly import Assembly
from helpers.log.logger import logger


class ReturnBuffAssemblyLineValues(SelectInfos):
    def __init__(self):
        self.log = logger("forecast")
        self.log.info("Inicializando ReturnBuffAssemblyLineValues")

        SelectInfos.__init__(self)

    def return_values_from_db(self):
        self.log.info("Montando query para retornar valores da linha de montagem (reception)")

        try:
            stmt = (
                select(
                    Assembly.knr,
                    Assembly.model,
                    Assembly.lfdnr_sequence,
                )
                .where(
                    Assembly.lane == "reception"
                )
            )
            self.log.info("Query SQL montada com sucesso")

        except Exception:
            self.log.error("Erro ao montar query SQL em return_values_from_db", exc_info=True)
            raise

        try:
            df = self.select(stmt)
            self.log.info(f"Select concluído — registros retornados: {df.height()}")
            return df

        except Exception:
            self.log.error("Erro ao executar SELECT da linha de montagem", exc_info=True)
            raise