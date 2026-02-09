from sqlalchemy import update
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session
from database.database import Base
import polars as pl


class UpsertInfos:
    def __init__(self, db: Session):
        self.db = db

    def upsert_df(self, table, df: pl.DataFrame, batch_size=1000):
        if isinstance(df, pl.LazyFrame):
            df = df.collect()

        total = len(df)
        rows = df.to_dicts()

        sql_table = Base.metadata.tables[table]

        for i in range(0, total, batch_size):
            batch = rows[i : i + batch_size]
            self._upsert_batch(sql_table, batch)

        return total

    def _upsert_batch(self, table, rows):
        stmt = insert(table).values(rows)

        fields_to_ignore = {"created_at", "updated_at"}

        update_cols = {
            col.name: stmt.inserted[col.name]
            for col in table.columns
            if not col.primary_key and col.name not in fields_to_ignore
        }

        stmt = stmt.on_duplicate_key_update(**update_cols)

        self.db.execute(stmt)
        self.db.commit()


class SelectInfos:
    def __init__(self, db: Session):
        self.db = db

    def select(self, query):
        result = self.db.execute(query)
        rows = result.fetchall()
        cols = result.keys()
        return pl.DataFrame(rows, schema=cols).lazy()


class UpdateInfos:
    def __init__(self, db: Session):
        self.db = db

    def update_df(self, table_name, df: pl.DataFrame, key_column, batch_size=1000):
        if isinstance(df, pl.LazyFrame):
            df = df.collect()

        table = Base.metadata.tables[table_name]
        rows = df.to_dicts()
        total = len(rows)

        for i in range(0, total, batch_size):
            batch = rows[i : i + batch_size]
            self._update_batch(table, batch, key_column)

        return total

    def _update_batch(self, table, batch, key_column):
        for row in batch:
            stmt = (
                update(table)
                .where(table.c[key_column] == row[key_column])
                .values({k: v for k, v in row.items() if k != key_column})
            )
            self.db.execute(stmt)

        self.db.commit()
