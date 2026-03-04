from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # APP CONFIG
    APP_NAME: str
    APP_URL: str
    FILES_DRIVER: str

    PKMC_URL: str
    PK05_URL: str

    # MYSQL
    MYSQL_HOST: str
    MYSQL_PORT: int
    MYSQL_USER: str
    MYSQL_PSWD: str
    MYSQL_DATABASE: str

    # STORAGE
    STORAGE_PATH: str
    EXCEL_STORAGE_PATH: str
    SAP_STORAGE_PATH: str

    # EXCEL FILES
    FX4PD_PATH: str

    # SAP
    LT22_PATH: str

    # ASSEMBLY LINE CONFIG
    AL_API_ENDPOINT: str

    # SAP LOGIN
    SAP_PATH: str
    SAP_CONNECTION_NAME: str
    SAP_USER: str
    SAP_PSWD: str

    class Config:
        env_file = "config/.env"
        extra = "forbid"
        case_sensitive = True


settings = Settings()