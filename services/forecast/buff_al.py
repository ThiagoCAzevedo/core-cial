from database.queries import SelectInfos
from sqlalchemy import select
from database.models.assembly import Assembly


class ReturnBuffAssemblyLineValues(SelectInfos):
    def __init__(self):
        SelectInfos.__init__(self)

    def return_values_from_db(self):
        stmt =  (
            select(
                Assembly.knr,
                Assembly.model,
                Assembly.lfdnr_sequence,
            )
            .where(
                Assembly.lane == "reception"
            )
        )
        return self.select(stmt)