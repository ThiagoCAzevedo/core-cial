from __future__ import annotations
from common.logger import logger
from modules.assembly.infrastructure.http_client import AssemblyApiClient
from modules.assembly.infrastructure.processor import AssemblyLazyExtractor, AssemblyLazyTransformer
import polars as pl


class AssemblyPipeline:
    def __init__(self):
        self.log = logger("assembly")

    def run(self, client: AssemblyApiClient) -> pl.DataFrame:
        self.log.info("Pipeline Assembly iniciado")

        raw = client.get_json()

        lf = AssemblyLazyExtractor(raw).extract()

        transformer = AssemblyLazyTransformer(lf)
        lf = transformer.transform()
        lf = AssemblyLazyTransformer(lf).attach_fx4pd()

        df = lf.collect()

        self.log.info(f"Pipeline finalizado — linhas: {df.height}")
        return df