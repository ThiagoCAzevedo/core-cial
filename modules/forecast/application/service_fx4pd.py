from modules.forecast.infrastructure.loaders import ForecastLoaders
from common.logger import logger
import polars as pl


class FX4PDService():
    def __init__(self, loader: ForecastLoaders):
        self.loader = loader
        self.log = logger("forecast")

    def create_fx4pd_df(self):
        return self.loader.load_fx4pd()

    def rename_select_columns(self, df):
        rename_map = {
            df.columns[0]: "knr_fx4pd",
            df.columns[1]: "partnumber",
            df.columns[5]: "qty_usage",
            df.columns[6]: "qty_unit",
        }
        return self._rename(df, rename_map)

    def clean_column(self, df):
        df = df.with_columns(pl.col(pl.Utf8).str.replace_all(" ", ""))
        df = df.filter(pl.col("qty_usage").cast(pl.Float64).is_not_null())
        df = df.with_columns(
            qty_usage=pl.col("qty_usage").cast(pl.Float64).fill_null(0.0),
            qty_unit=pl.col("qty_unit").cast(pl.Int32).fill_null(0),
        )
        return df