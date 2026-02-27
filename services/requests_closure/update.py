from database.queries import SelectInfos, UpdateInfos, DeleteInfos
import polars as pl
from sqlalchemy import select
from database.models.requests_made import RequestsMade
from database.models.pkmc import PKMC


class ValuesToQuery:
    def __init__(self, session):
        self.session = session
        self.selector = SelectInfos(session)

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
        return self.selector.select(stmt)

    def _return_values_pkmc(self):
        stmt = (
            select(
                PKMC.partnumber,
                PKMC.lb_balance,
                PKMC.rack,
                PKMC.supply_area
            )
        )
        return self.selector.select(stmt)

    def join_lt22_requests_made(self, df_lt22):
        df_requests = self._return_requests_made_values()
        return df_requests.join(
            df_lt22,
            on=["partnumber", "supply_area"],
            how="inner"
        )

    def join_requests_pkmc(self, df_requests):
        df_pkmc = self._return_values_pkmc()
        return df_pkmc.join(
            df_requests,
            on=["partnumber", "rack"],
            how="inner"
        )


class UpdateDeleteValues:
    def __init__(self, session):
        self.session = session
        self.updater = UpdateInfos(session)
        self.deleter = DeleteInfos(session)

    def update_lb_balance(self, df_lt22):

        vtq = ValuesToQuery(self.session)

        df_join = vtq.join_lt22_requests_made(df_lt22).lazy()

        df_pkmc = vtq.join_requests_pkmc(df_join).lazy()

        df_totals = (
            df_join
            .groupby(["partnumber", "rack"])
            .agg(pl.col("qty_to_request").sum().alias("qty_to_request"))
            .lazy()
        )

        df_final = (
            df_pkmc
            .join(df_totals, on=["partnumber", "rack"], how="left")
            .with_columns(
                (
                    pl.col("lb_balance") + pl.col("qty_to_request").fill_null(0)
                ).alias("lb_balance")
            )
            .select([
                "partnumber",
                "supply_area",
                "lb_balance"
            ])
        )

        df_final = df_final.collect()

        self.updater.update_df(
            table_name="pkmc",
            df=df_final,
            key_column="partnumber"
        )
        return df_final

    def delete_requests_made(self, df_lt22):
        vtq = ValuesToQuery(self.session)
        df_join = vtq.join_lt22_requests_made(df_lt22)

        self.deleter.delete_df(
            table_name="requests_made",
            df=df_join,
            key_column="partnumber"
        )