from pydantic import BaseSettings

class Settings(BaseSettings):
    full_external_api_url: str
    postgres_user: str
    postgres_password: str
    postgres_db: str
    postgres_host: str = "db"
    postgres_port: int = 5432
    arc_batch_size: int = 2000

    class Config:
        env_file = ".env"

settings = Settings()
