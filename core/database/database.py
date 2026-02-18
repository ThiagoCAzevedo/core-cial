from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
from helpers.log.logger import logger
import os


log = logger("database")
log.info("Loading environment variables for database connection")

try:
    load_dotenv("config/.env")
    log.info(".env file loaded successfully")
except Exception:
    log.error("Error loading .env file", exc_info=True)
    raise

try:
    DATABASE_URL = (
        f"mysql+mysqlconnector://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PSWD')}"
        f"@{os.getenv('MYSQL_HOST')}/{os.getenv('MYSQL_DATABASE')}"
    )
    log.info("DATABASE_URL successfully assembled")

except Exception:
    log.error("Error assembling DATABASE_URL", exc_info=True)
    raise

try:
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=3600,
    )
    log.info("SQLAlchemy engine created successfully")

except Exception:
    log.error("Error creating SQLAlchemy engine", exc_info=True)
    raise

try:
    SessionLocal = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
    )
    log.info("SessionLocal configured successfully")

except Exception:
    log.error("Error configuring SessionLocal", exc_info=True)
    raise

try:
    Base = declarative_base()
    log.info("Declarative Base initialized successfully")

except Exception:
    log.error("Error initializing Declarative Base", exc_info=True)
    raise


def get_db():
    log.info("Creating database session")
    db = SessionLocal()
    try:
        yield db
    except Exception:
        log.error("Error during database session usage", exc_info=True)
        raise
    finally:
        db.close()
        log.info("Database session closed")