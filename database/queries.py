from sqlalchemy import update, select, delete
from sqlalchemy.sql import Select
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session
from database.database import Base
from helpers.log.logger import logger
import polars as pl


class UpsertInfos:
    def __init__(self, db: Session):
        self.db = db
        self.log = logger("database")
        self.log.info("Initializing UpsertInfos")

    def upsert_df(self, table, df: pl.DataFrame, batch_size=1000):
        self.log.info(f"Starting UPSERT operation for table '{table}'")

        try:
            if isinstance(df, pl.LazyFrame):
                df = df.collect()

            rows = df.to_dicts()
            total = len(rows)
            sql_table = Base.metadata.tables[table]

            self.log.info(f"Total records for UPSERT: {total}")

        except Exception:
            self.log.error("Error preparing data for UPSERT", exc_info=True)
            raise

        try:
            for i in range(0, total, batch_size):
                batch = rows[i:i + batch_size]
                self.log.info(f"UPSERT batch {i} - {i + len(batch)}")
                self._upsert_batch(sql_table, batch)

            self.log.info("UPSERT operation completed successfully")

        except Exception:
            self.log.error("Error executing UPSERT batch", exc_info=True)
            raise

        return total

    def _upsert_batch(self, table, rows):
        try:
            stmt = insert(table).values(rows)

            ignore_fields = {"created_at", "updated_at"}

            update_cols = {
                col.name: stmt.inserted[col.name]
                for col in table.columns
                if not col.primary_key and col.name not in ignore_fields
            }

            stmt = stmt.on_duplicate_key_update(**update_cols)

            self.db.execute(stmt)
            self.db.commit()

        except Exception:
            self.log.error("Error executing UPSERT batch", exc_info=True)
            raise



class SelectInfos:
    def __init__(self, db):
        self.db = db
        self.log = logger("database")
        self.log.info("Initializing SelectInfos")

    def select(
        self,
        query_or_table,
        columns: list[str] = None,
        filters: dict = None
    ):
        if isinstance(query_or_table, Select):
            stmt = query_or_table
        else:
            table_name = query_or_table
            table = Base.metadata.tables[table_name]

            if columns:
                col_objs = [table.c[col] for col in columns]
            else:
                col_objs = [table]

            stmt = select(*col_objs)

            if filters:
                for col, value in filters.items():
                    stmt = stmt.where(table.c[col] == value)

        try:
            rows = self.db.execute(stmt).mappings().all()
            return pl.DataFrame(rows) if rows else pl.DataFrame()

        except Exception:
            self.log.error("Error executing SELECT", exc_info=True)
            raise


class UpdateInfos:
    def __init__(self, db: Session):
        self.db = db
        self.log = logger("database")
        self.log.info("Initializing UpdateInfos")

    def update_df(self, table_name, df: pl.DataFrame, key_column, batch_size=1000):
        self.log.info(f"Starting UPDATE operation for table '{table_name}'")

        try:
            if isinstance(df, pl.LazyFrame):
                df = df.collect()

            table = Base.metadata.tables[table_name]
            rows = df.to_dicts()
            total = len(rows)

            self.log.info(f"Total records for UPDATE: {total}")

        except Exception:
            self.log.error("Error preparing data for UPDATE", exc_info=True)
            raise

        try:
            for i in range(0, total, batch_size):
                batch = rows[i:i + batch_size]
                self.log.info(f"UPDATE batch {i} - {i + len(batch)}")
                self._update_batch(table, batch, key_column)

            self.log.info("UPDATE completed successfully")

        except Exception:
            self.log.error("Error executing UPDATE batches", exc_info=True)
            raise

        return total

    def _update_batch(self, table, batch, key_column):
        try:
            for row in batch:
                key_value = row[key_column]

                update_values = {k: v for k, v in row.items() if k != key_column}

                stmt = (
                    update(table)
                    .where(table.c[key_column] == key_value)
                    .values(update_values)
                )

                self.db.execute(stmt)

            self.db.commit()
            self.log.info(f"Batch updated: {len(batch)} records")

        except Exception:
            self.log.error("Error executing UPDATE batch", exc_info=True)
            raise


class DeleteInfos:
    def __init__(self, db: Session):
        self.db = db
        self.log = logger("database")
        self.log.info("Initializing DeleteInfos")

    def delete_df(self, table_name, df: pl.DataFrame, key_column, batch_size=1000):
        self.log.info(f"Starting DELETE operation for table '{table_name}'")

        try:
            if isinstance(df, pl.LazyFrame):
                df = df.collect()

            table = Base.metadata.tables[table_name]
            rows = df.to_dicts()
            total = len(rows)

            self.log.info(f"Total records for DELETE: {total}")

        except Exception:
            self.log.error("Error preparing data for DELETE", exc_info=True)
            raise

        try:
            for i in range(0, total, batch_size):
                batch = rows[i:i + batch_size]
                self.log.info(f"DELETE batch {i} - {i + len(batch)}")
                self._delete_batch(table, batch, key_column)

            self.log.info("DELETE completed successfully")

        except Exception:
            self.log.error("Error executing DELETE batches", exc_info=True)
            raise

        return total

    def _delete_batch(self, table, batch, key_column):
        try:
            for row in batch:
                key_value = row[key_column]

                stmt = delete(table).where(table.c[key_column] == key_value)

                self.db.execute(stmt)

            self.db.commit()
            self.log.info(f"Batch deleted: {len(batch)} records")

        except Exception:
            self.log.error("Error executing DELETE batch", exc_info=True)
            raise