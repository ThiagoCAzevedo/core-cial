import polars as pl
from helpers.log.logger import logger


class DefineDataFrame:
    def __init__(self, response: dict):
        self.log = logger("assembly")
        self.log.info("Initializing DefineDataFrame")

        try:
            self.response = response
            self.log.info("Response received for processing")
        except Exception:
            self.log.error("Error initializing response in DefineDataFrame", exc_info=True)
            raise

    def extract_car_records(self):
        self.log.info("Starting CAR record extraction")

        try:
            registers = []

            for lane_key, lane_val in self.response.items():
                if lane_key.startswith("lane_") or lane_key.startswith("reception"):
                    for fb_key, fb_val in lane_val.items():
                        for tact_key, tact_val in fb_val.items():
                            if isinstance(tact_val, dict) and "CAR" in tact_val and tact_val["CAR"]:
                                car = tact_val["CAR"]

                                registers.append({
                                    "knr": car.get("KNR"),
                                    "model": car.get("MODELL"),
                                    "lfdnr_sequence": car.get("LFDNR"),
                                    "werk": car.get("WERK"),
                                    "spj": car.get("SPJ"),
                                    "lane": tact_val.get("LANE", lane_key),
                                    "takt": tact_val.get("TACT"),
                                })

            df = pl.DataFrame(registers)

            self.log.info(f"Total CAR records extracted: {df.height}")

            return df

        except Exception:
            self.log.error("Error extracting CAR records from cleaned JSON", exc_info=True)
            raise


class TransformDataFrame:
    def __init__(self, df):
        self.log = logger("assembly")
        self.log.info("Initializing TransformDataFrame")

        try:
            self.df = df
            self.log.info(
                f"DataFrame received — rows: {df.height}, columns: {len(df.columns)}"
            )
        except Exception:
            self.log.error("Error initializing DataFrame in TransformDataFrame", exc_info=True)
            raise

    def transform(self):
        self.log.info("Applying transformations (remove lane_ prefix, cast lfdnr_sequence)")

        try:
            df = (
                self.df
                .with_columns([
                    pl.col("lane").str.replace("lane_", ""),
                    pl.col("lfdnr_sequence").cast(pl.Utf8)
                ])
            )

            self.log.info("Transformation completed successfully")
            return df

        except Exception:
            self.log.error(
                "Error transforming DataFrame in TransformDataFrame.transform()",
                exc_info=True
            )
            raise
    
    def attach_fx4pd(self):
        self.log.info("Creating column knr_fx4pd")

        try:
            df = self.df.with_columns(
                (pl.col("werk") + pl.col("spj") + pl.col("knr")).alias("knr_fx4pd")
            )

            self.log.info("Column knr_fx4pd created successfully")
            return df

        except Exception:
            self.log.error(
                "Error creating knr_fx4pd column in TransformDataFrame.attach_fx4pd()",
                exc_info=True
            )
            raise