from common.logger import logger
from modules.forecast.application.service_fx4pd import FX4PDService


class ForecastPipeline:
    def __init__(self):
        self.log = logger("forecast")

    def build_fx4pd(self, svc: FX4PDService):
        lf = svc.create_fx4pd_df()
        lf = svc.rename_select_columns(lf)
        lf = svc.clean_column(lf)
        return lf