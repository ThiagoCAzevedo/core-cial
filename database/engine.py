from sqlalchemy import create_engine
from common.logger import logger
from config.settings import settings


log = logger("database")

DATABASE_URL = (
    f"mysql+mysqlconnector://"
    f"{settings.MYSQL_USER}:{settings.MYSQL_PSWD}@"
    f"{settings.MYSQL_HOST}:{settings.MYSQL_PORT}/"
    f"{settings.MYSQL_DATABASE}"
)

log.info(f"Connecting to database → host={settings.MYSQL_HOST}")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=3600,
)
