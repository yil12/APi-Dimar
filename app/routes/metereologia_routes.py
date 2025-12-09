from fastapi import APIRouter, Query, HTTPException, Depends
from sqlalchemy.orm import Session
from fastapi.responses import StreamingResponse
import openpyxl
from openpyxl.workbook import Workbook
from io import BytesIO
from app.services.db_service import (
    get_unique_years,
    get_unique_stations_by_year
)

router = APIRouter(prefix="", tags=["Antartica Meteorologia"])

@router.get("/anios", summary="Obtener años únicos con datos meteorológicos")
async def fetch_unique_years():
    """
    Devuelve una lista ordenada de años en los que hay registros meteorológicos.
    Ejemplo: [2017, 2018, 2020, 2022]
    """
    years = await get_unique_years()
    return {
        "total": len(years),
        "anios": years
    }

@router.get("/anio/{year}/estaciones", summary="Estaciones únicas con datos en un año dado")
async def fetch_stations_by_year(year: int):
    """
    Devuelve una FeatureCollection de GeoJSON con una feature por estación
    que tenga al menos un registro en el año especificado.
    Incluye geometría (coordenadas) y propiedades meteorológicas.
    """
    features = await get_unique_stations_by_year(year)
    return {
        "anio": year,
        "total_estaciones": len(features),
        "type": "FeatureCollection",
        "features": features
    }