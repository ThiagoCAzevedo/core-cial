from database.queries import SelectInfos
from sqlalchemy import select
from database.models.assembly import Assembly
from helpers.log.logger import logger


class ReturnBuffAssemblyLineValues(SelectInfos):
    def __init__(self):
        self.log = logger("forecast")
        self.log.info("Initializing ReturnBuffAssemblyLineValues")

        SelectInfos.__init__(self)

    def return_values_from_db(self):
        self.log.info("Building query to return assembly line values (reception)")

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
            self.log.info("SQL query successfully built")

        except Exception:
            self.log.error("Error building SQL query in return_values_from_db", exc_info=True)
            raise

        try:
            df = self.select(stmt)
            self.log.info(f"Select completed — records returned: {df.height()}")
            return df

        except Exception:
            self.log.error("Error executing SELECT for assembly line", exc_info=True)
            raise