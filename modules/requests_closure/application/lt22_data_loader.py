from common.utils.cleaner import CleanerBase
from common.logger import logger


class DefineDataFrame(CleanerBase):
    def __init__(self):
        self.log = logger("requests_closure")
        self.log.info("Initializing DefineDataFrame")
        super().__init__()

    def create_lt22_df(self):
        self.log.info("Loading LT22 file and searching for header row")
        try:
            rows_to_skip = 0
            while True:
                df = self._load_file("LT22_PATH", rows_to_skip=rows_to_skip, separator="\t")
                if any("OT" in col for col in df.columns):
                    self.log.info(f"Header found after skipping {rows_to_skip} rows")
                    break
                rows_to_skip += 1

            self.log.info("LT22 DataFrame loaded successfully")
            return df.lazy()
        except Exception:
            self.log.error("Error loading LT22 file", exc_info=True)
            raise
