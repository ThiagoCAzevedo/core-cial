from sqlalchemy import update
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session
from database.database import Base
from helpers.log.logger import logger
import polars as pl


class UpsertInfos:
    def __init__(self, db: Session):
        self.db = db
        self.log = logger("database")
        self.log.info("Inicializando UpsertInfos")

    def upsert_df(self, table, df: pl.DataFrame, batch_size=1000):
        self.log.info(f"Iniciando UPSERT no banco para a tabela '{table}'")

        try:
            if isinstance(df, pl.LazyFrame):
                self.log.info("Convertendo LazyFrame para DataFrame")
                df = df.collect()

            total = len(df)
            self.log.info(f"Total de registros para upsert: {total}")

            rows = df.to_dicts()
            sql_table = Base.metadata.tables[table]

        except Exception:
            self.log.error("Erro ao preparar dados para UPSERT", exc_info=True)
            raise

        try:
            for i in range(0, total, batch_size):
                batch = rows[i: i + batch_size]
                self.log.info(f"Executando UPSERT para batch {i} - {i + len(batch)}")
                self._upsert_batch(sql_table, batch)

            self.log.info("UPSERT finalizado com sucesso")

        except Exception:
            self.log.error("Erro ao executar batches de UPSERT", exc_info=True)
            raise

        return total

    def _upsert_batch(self, table, rows):
        try:
            self.log.info(f"Montando instrução UPSERT para {len(rows)} registros")

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

            self.log.info("Batch de UPSERT executado e commit realizado")

        except Exception:
            self.log.error("Erro ao executar UPSERT no batch", exc_info=True)
            raise


class SelectInfos:
    def __init__(self, db: Session):
        self.db = db
        self.log = logger("database")
        self.log.info("Inicializando SelectInfos")

    def select(self, query):
        self.log.info("Executando SELECT no banco")

        try:
            result = self.db.execute(query)
            rows = result.fetchall()
            cols = result.keys()

            self.log.info(f"SELECT concluído com {len(rows)} registros")

            return pl.DataFrame(rows, schema=cols).lazy()

        except Exception:
            self.log.error("Erro ao executar SELECT", exc_info=True)
            raise


class UpdateInfos:
    def __init__(self, db: Session):
        self.db = db
        self.log = logger("database")
        self.log.info("Inicializando UpdateInfos")

    def update_df(self, table_name, df: pl.DataFrame, key_column, batch_size=1000):
        self.log.info(f"Iniciando UPDATE na tabela '{table_name}'")

        try:
            if isinstance(df, pl.LazyFrame):
                self.log.info("Convertendo LazyFrame para DataFrame")
                df = df.collect()

            table = Base.metadata.tables[table_name]
            rows = df.to_dicts()
            total = len(rows)

            self.log.info(f"Total de registros para update: {total}")

        except Exception:
            self.log.error("Erro ao preparar dados para UPDATE", exc_info=True)
            raise

        try:
            for i in range(0, total, batch_size):
                batch = rows[i: i + batch_size]
                self.log.info(f"Executando UPDATE para batch {i} - {i + len(batch)}")
                self._update_batch(table, batch, key_column)

            self.log.info("UPDATE finalizado com sucesso")

        except Exception:
            self.log.error("Erro ao executar batches de UPDATE", exc_info=True)
            raise

        return total

    def _update_batch(self, table, batch, key_column):
        try:
            for row in batch:
                stmt = (
                    update(table)
                    .where(table.c[key_column] == row[key_column])
                    .values({k: v for k, v in row.items() if k != key_column})
                )
                self.db.execute(stmt)

            self.db.commit()
            self.log.info(f"Batch UPDATE executado para {len(batch)} registros")

        except Exception:
            self.log.error("Erro ao executar UPDATE no batch", exc_info=True)
            raise