from database.queries import SelectInfos
from database.models.pkmc import PKMC
from database.models.requests_made import RequestsMade
from sqlalchemy import select, update, delete
import polars as pl


class ValuesToQuery(SelectInfos):
    def __init__(self):
        super().__init__()

    def _return_requests_made_values(self):
        stmt = (
            select(
                RequestsMade.partnumber,
                RequestsMade.supply_area,
                RequestsMade.num_reg_circ,
                RequestsMade.qty_to_request,
                RequestsMade.takt,
                RequestsMade.rack,
            )
        )

        result = self.session.execute(stmt).all()
        return pl.DataFrame([dict(r._mapping) for r in result])

    def join_lt22_requests_made(self, df_lt22: pl.DataFrame):
        df_requests_made = self._return_requests_made_values()

        return df_requests_made.join(
            df_lt22,
            on=["partnumber", "supply_area"],
            how="inner"
        )


class UpdateDeleteValues:
    def __init__(self, session):
        self.session = session

    def update_lb_balance(self, df_lt22):
        df_join = ValuesToQuery().join_lt22_requests_made(df_lt22)

        df_totals = (
            df_join.lazy()
            .groupby(["partnumber", "supply_area"])
            .agg(pl.col("qty_to_request").sum().alias("qty_to_request"))
            .collect()
        )

        for row in df_totals.iter_rows(named=True):
            stmt = (
                update(PKMC)
                .where(
                    PKMC.partnumber == row["partnumber"],
                    PKMC.supply_area == row["supply_area"],
                )
                .values(lb_balance=PKMC.lb_balance + row["qty_to_request"])
            )
            self.session.execute(stmt)

        self.session.commit()

    def delete_requests_made(self, df_lt22):
        df_join = ValuesToQuery().join_lt22_requests_made(df_lt22)

        for row in df_join.iter_rows(named=True):
            stmt = (
                delete(RequestsMade)
                .where(RequestsMade.partnumber == row["partnumber"])
            )
            self.session.execute(stmt)

        self.session.commit()