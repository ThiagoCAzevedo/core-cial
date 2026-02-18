from database.queries import SelectInfos, UpdateInfos, DeleteInfos
import polars as pl


class ValuesToQuery:
    def __init__(self, session):
        self.session = session
        self.selector = SelectInfos(session)

    def _return_requests_made_values(self):
        df = self.selector.select(
            table="requests_made",
            columns=[
                "partnumber",
                "supply_area",
                "num_reg_circ",
                "qty_to_request",
                "takt",
                "rack",
            ]
        )

        return df.collect()

    def join_lt22_requests_made(self, df_lt22: pl.DataFrame):
        df_requests = self._return_requests_made_values()

        return df_requests.join(
            df_lt22,
            on=["partnumber", "supply_area"],
            how="inner"
        )


class UpdateDeleteValues:
    def __init__(self, session):
        self.session = session
        self.updater = UpdateInfos(session)
        self.deleter = DeleteInfos(session)

    def update_lb_balance(self, df_lt22):
        vtq = ValuesToQuery(self.session)
        df_join = vtq.join_lt22_requests_made(df_lt22)

        df_totals = (
            df_join.lazy()
            .groupby(["partnumber", "supply_area"])
            .agg(pl.col("qty_to_request").sum().alias("qty_to_request"))
            .collect()
        )

        self.updater.update_df(
            table_name="pkmc",
            df=df_totals,
            key_column="partnumber"
        )

    def delete_requests_made(self, df_lt22):
        vtq = ValuesToQuery(self.session)
        df_join = vtq.join_lt22_requests_made(df_lt22)

        self.deleter.delete_df(
            table_name="requests_made",
            df=df_join,
            key_column="partnumber"
        )
