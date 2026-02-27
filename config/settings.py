from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    MYSQL_USER: str
    MYSQL_PSWD: str
    MYSQL_HOST: str
    MYSQL_PORT: int
    MYSQL_DATABASE: str

    AL_API_ENDPOINT: str

    class Config:
        env_file = "config/.env"


settings = Settings()