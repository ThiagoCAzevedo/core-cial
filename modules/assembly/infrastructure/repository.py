from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session
from common.logger import logger
from modules.assembly.application.dtos import AssemblyRecordDTO
from modules.assembly.infrastructure.models import Assembly


class AssemblyRepository:
    def __init__(self, db: Session):
        self.db = db
        self.log = logger("assembly")

    def bulk_upsert(self, records: list[AssemblyRecordDTO], batch_size: int = 5000) -> int:
        total = 0
        try:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]

                stmt = insert(Assembly).values(batch)
                ignore_cols = ["knr", "created_at", "updated_at"]
                update_stmt = {
                    col.name: stmt.inserted[col.name]
                    for col in Assembly.__table__.columns
                    if col.name not in ignore_cols
                }

                stmt = stmt.on_duplicate_key_update(update_stmt)

                self.db.execute(stmt)
                self.db.commit()

                total += len(batch)
                self.log.info(f"Upsert batch finished - rows {len(batch)}")

            return total

        except Exception:
            self.db.rollback()
            self.log.error("Error in bulk_upsert", exc_info=True)
            raise