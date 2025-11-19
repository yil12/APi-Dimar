from fastapi import FastAPI
from app.core.config import settings
from app.routes.antartica_routes import router as ant_router

app = FastAPI(
    title="API Antártica (cached DB)",
    description="FastAPI servicio que sirve datos convertidos desde ArcGIS (guardados en Postgres).",
    version="1.0",
    docs_url="/swagger",
    redoc_url="/redoc"
)

app.include_router(ant_router)

@app.get("/")
def root():
    return {"mensaje": "API Antártica lista", "arcgis": settings.full_external_api_url}
