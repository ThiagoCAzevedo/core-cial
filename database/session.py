from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from common.logger import logger
from database.engine import engine

log = logger("database")

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)


def get_db() -> Session:
    db = SessionLocal()
    log.debug("Database session opened")
    try:
        yield db
    finally:
        db.close()
        log.debug("Database session closed")