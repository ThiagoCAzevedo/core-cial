from sqlalchemy import select
from modules.assembly.infrastructure.models import Assembly
from common.logger import logger
import polars as pl


class BuffALService:
    def __init__(self, db):
        self.db = db
        self.log = logger("forecast")

    def return_values_from_db(self):
        stmt = (
            select(
                Assembly.knr,
                Assembly.knr_fx4pd,
                Assembly.model,
                Assembly.lfdnr_sequence,
            )
            .where(Assembly.lane == "reception")
        )

        rows = self.db.execute(stmt).all()
        return pl.DataFrame(rows).lazy()