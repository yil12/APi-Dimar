from pydantic import BaseSettings
import os
from dotenv import load_dotenv

# Cargar variables desde .env
load_dotenv()

class Settings(BaseSettings):
    full_external_api_url: str
    postgres_user: str
    postgres_password: str
    postgres_db: str
    postgres_host: str = "db"
    postgres_port: int = 5432
    arc_batch_size: int = 2000

    APP_NAME: str = os.getenv("APP_NAME", "Mi API")
    APP_VERSION: str = os.getenv("APP_VERSION", "1.0.0")
    APP_DESCRIPTION: str = os.getenv("APP_DESCRIPTION", "Descripción por defecto")
    APP_PORT: int = int(os.getenv("APP_PORT", 8080))
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"

    EXTERNAL_API_BASE_URL_METEROLOGIA: str = os.getenv("EXTERNAL_API_BASE_URL_METEROLOGIA")
    ##EXTERNAL_API_BASE_URL: str = os.getenv("EXTERNAL_API_BASE_URL")
    EXTERNAL_API_LAYER: str = os.getenv("EXTERNAL_API_LAYER")
    EXTERNAL_API_ENDPOINT: str = os.getenv("EXTERNAL_API_ENDPOINT")

    @property
    def full_external_api_url(self) -> str:
        return f"{self.EXTERNAL_API_BASE_URL_METEROLOGIA}/{self.EXTERNAL_API_LAYER}/{self.EXTERNAL_API_ENDPOINT}"

    class Config:
        env_file = ".env"

settings = Settings()
