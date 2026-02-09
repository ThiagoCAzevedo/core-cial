from fastapi import Depends
from services.assembly.assembly_api import AccessAssemblyLineApi
from services.assembly.processor import DefineDataFrame, TransformDataFrame
from database.queries import UpsertInfos
from database.database import get_db
from sqlalchemy.orm import Session



class BuildPipeline:
    def build_assembly(api: AccessAssemblyLineApi):
        raw = api.get_raw_response()
        df = DefineDataFrame(raw).extract_car_records()
        df = TransformDataFrame(df).transform()
        df = TransformDataFrame(df).attach_fx4pd()
        return df.collect()
    

class DependeciesInjection:
    @staticmethod
    def get_api() -> AccessAssemblyLineApi:
        return AccessAssemblyLineApi()

    @staticmethod
    def get_upsert(db: Session = Depends(get_db)) -> UpsertInfos:
        return UpsertInfos(db)