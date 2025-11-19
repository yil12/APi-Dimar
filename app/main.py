from fastapi import FastAPI
from app.core.config import settings
from app.routes.antartica_routes import router as ant_router
from app.db import Base, engine
from sqlalchemy import text

app = FastAPI(
    title="API Antártica (cached DB)",
    description="FastAPI servicio que sirve datos convertidos desde ArcGIS (guardados en Postgres).",
    version="1.0",
    docs_url="/swagger",
    redoc_url="/redoc"
)

# Crear las tablas al iniciar
@app.on_event("startup")
def startup_event():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(" Conectado a la base de datos correctamente.")
    except Exception as e:
        print(" Error al conectar a la base de datos:")
        print(e)

app.include_router(ant_router)

@app.get("/")
def root():
    return {
        "mensaje": "API Antártica lista",
        "arcgis": settings.full_external_api_url
    }
