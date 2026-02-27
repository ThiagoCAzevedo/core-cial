from __future__ import annotations
from sqlalchemy.orm import Session
from common.logger import logger
from modules.assembly.application.dtos import AssemblyRecordDTO
from modules.assembly.infra.models import AssemblyModel  # opcional, posso gerar para você


class AssemblyRepository:
    def __init__(self, db: Session):
        self.db = db
        self.log = logger("assembly")

    def bulk_upsert(self, records: list[AssemblyRecordDTO], batch_size: int = 5000) -> int:
        total = 0
        try:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]

                self.db.bulk_insert_mappings(
                    AssemblyModel,
                    batch,
                )
                self.db.commit()

                total += len(batch)
                self.log.info(f"Upsert batch concluído: {len(batch)} registros")

            return total

        except Exception as exc:
            self.db.rollback()
            self.log.error("Erro no bulk_upsert", exc_info=True)
            raise exc