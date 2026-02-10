from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
from helpers.log.logger import logger
import os


log = logger("database")
log.info("Carregando variáveis de ambiente para conexão com o banco")


try:
    load_dotenv("config/.env")
    log.info(".env carregado com sucesso")
except Exception:
    log.error("Erro ao carregar .env", exc_info=True)
    raise


try:
    DATABASE_URL = (
        f"mysql+mysqlconnector://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PSWD')}"
        f"@{os.getenv('MYSQL_HOST')}/{os.getenv('MYSQL_DATABASE')}"
    )
    log.info("DATABASE_URL montada com sucesso")

except Exception:
    log.error("Erro ao montar DATABASE_URL", exc_info=True)
    raise


try:
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=3600,
    )
    log.info("Engine do SQLAlchemy criada com sucesso")

except Exception:
    log.error("Erro ao criar engine do SQLAlchemy", exc_info=True)
    raise


try:
    SessionLocal = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
    )
    log.info("SessionLocal configurada com sucesso")

except Exception:
    log.error("Erro ao configurar SessionLocal", exc_info=True)
    raise


try:
    Base = declarative_base()
    log.info("Declarative Base inicializada")

except Exception:
    log.error("Erro ao inicializar Declarative Base", exc_info=True)
    raise


def get_db():
    log.info("Criando sessão com o banco")
    db = SessionLocal()
    try:
        yield db
    except Exception:
        log.error("Erro durante uso da sessão do banco", exc_info=True)
        raise
    finally:
        db.close()
        log.info("Sessão com o banco encerrada")