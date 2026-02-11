from helpers.data.cleaner import CleanerBase
import polars as pl
import re

class DefineDataFrame(CleanerBase):
    def __init__(self):
        CleanerBase.__init__(self)

    def create_lt22_df(self):
        rows_to_skip = 0
        while True:
            df = self._load_file("LT22_PATH", rows_to_skip=rows_to_skip, separator="\t")
            if any("OT" in col for col in df.columns):
                break
            rows_to_skip += 1
        return df.lazy()
    

class CleanDataFrame(CleanerBase):
    def __init__(self):
        CleanerBase.__init__(self)

    def clean_columns(self, df):
        df = df.with_columns(
            pl.col("partnumber")
                .cast(pl.Utf8)
                .str.strip()         
                .str.replace_all(r"\s+", "")
                .str.replace_all(r"\.", "")
                .str.replace_all(r"[^\w-]", "")
                .str.to_uppercase(),
            
            pl.col("num_ot")
                .cast(pl.Utf8)
                .str.strip()         
                .str.replace_all(r"\s+", "")
                .str.replace_all(r"\.", "")
                .str.replace_all(r"[^\w-]", "")
                .str.to_uppercase()
        )
        return df

    def filter_values(self, df):
        return df.filter(pl.col("deposit_type") == "B01")
    
    def change_columns_type(self, df):
        return df.with_columns(
            pl.col("request_date").str.strptime(pl.Date, format="%d.%m.%Y", strict=False),
            pl.col("request_hour").str.strptime(pl.Time, format="%H:%M:%S", strict=False)
        )

    def rename_columns(self, df):
        rename_map = {
            "N� OT": "num_ot",
            "Material": "partnumber",
            "Hora": "request_hour",
            "Dt.cria��o": "request_date",
            "Tp._duplicated_0": "deposit_type",
            "Posi�Dest": "supply_area"
        }

        return self._rename(df, rename_map)


# define_dataframe = DefineDataFrame()
# clean_df = CleanDataFrame()

# df = define_dataframe.find_start_lt22()

# df = clean_df.rename_columns(df)
# df = clean_df.filter_values(df)
# df = clean_df.clean_columns(df)
# df = clean_df.change_columns_type(df)

# print(df.collect())