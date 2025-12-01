from fastapi import APIRouter, Query, HTTPException, Depends
from sqlalchemy.orm import Session
from fastapi.responses import StreamingResponse
import openpyxl
from openpyxl.workbook import Workbook
from io import BytesIO
from app.db import get_db
from app.services import db_service
from app.utils.geojson_converter import features_to_geojson_from_db

router = APIRouter(prefix="/meteorologico", tags=["Meteorologico"])

@router.get("/estacion/{nombre}")
def endpoint_by_station(nombre: str, limit: int = 100, offset: int = 0):
    rows = db_service.get_by_station(nombre, limit=limit, offset=offset)
    if not rows:
        return {"exito": True, "count": 0, "type": "FeatureCollection", "features": []}
    features = features_to_geojson_from_db(rows)
    return {"exito": True, "count": len(features), "type": "FeatureCollection", "features": features}

@router.get("/stations")
def endpoint_all_stations(db: Session = Depends(get_db)):
    stations = db_service.get_all_stations(db)
    return {
        "total": len(stations),
        "stations": stations
    }

@router.get("/objectid/{oid}")
def endpoint_objectid(oid: int):
    row = db_service.get_by_objectid(oid)
    if not row:
        raise HTTPException(status_code=404, detail="objectid not found")
    feature = features_to_geojson_from_db([row])[0]
    return {"type": "FeatureCollection", "features": [feature]}

@router.get("/years")
def endpoint_years(db: Session = Depends(get_db)):
    years = db_service.get_years(db)
    return {"years": years}

@router.get("/profundidades")
def endpoint_depths():
    depths = db_service.get_depths()
    return {"exito": True, "profundidades": depths}

@router.get("/profundidad/{valor}")
def endpoint_by_depth(valor: float, db: Session = Depends(get_db)):
    rows = db_service.get_by_depth(db, valor)

    if not rows:
        return {
            "exito": True,
            "count": 0,
            "type": "FeatureCollection",
            "features": []
        }

    # Convertimos a GeoJSON (usando tu utilitario)
    features = features_to_geojson_from_db(rows)

    return {
        "exito": True,
        "count": len(features),
        "type": "FeatureCollection",
        "features": features
    }

@router.get("/estacion/{nombre}/profundidades")
def endpoint_station_depths(nombre: str, db: Session = Depends(get_db)):
    depths = db_service.get_depths_by_station(db, nombre)

    return {
        "exito": True,
        "estacion": nombre,
        "total": len(depths),
        "profundidades": depths
    }


@router.get("/filtrar")
def endpoint_data_by_year_station(
    station: str,
    year: int,
    db: Session = Depends(get_db)
):
    rows = db_service.get_by_year_and_station(db, year, station)
    return {
        "exito": True,
        "station": station,
        "year": year,
        "total": len(rows),
        "features": features_to_geojson_from_db(rows)
    }

@router.get("/years/{station}")
def endpoint_years_by_station(station: str, db: Session = Depends(get_db)):
    years = db_service.get_years_by_station(db, station)
    return {
        "station": station,
        "total_years": len(years),
        "years": years
    }

@router.get("/mediciones/por-anio/{year}")
def endpoint_measurements_by_year(
    year: int,
    page: int = 1,
    limit: int = 200,
    db: Session = Depends(get_db)
):
    total, rows = db_service.get_measurements_by_year(db, year, page, limit)

    return {
        "exito": True,
        "year": year,
        "page": page,
        "limit": limit,
        "total_registros": total,
        "total_paginas": (total // limit) + 1,
        "features": features_to_geojson_from_db(rows)
    }

@router.get("/estaciones/por-anio/{year}")
def endpoint_first_records_by_year(
    year: int,
    db: Session = Depends(get_db)
):
    rows = db_service.get_first_records_by_year(db, year)

    features = features_to_geojson_from_db(rows)

    return {
        "exito": True,
        "year": year,
        "total": len(features),
        "type": "FeatureCollection",
        "features": features
    }

@router.get("/por-anio-y-estacion")
def endpoint_by_year_and_station(
    year: int,
    station: str,
    db: Session = Depends(get_db)
):
    rows = db_service.get_records_by_year_and_station(db, year, station)

    if not rows:
        return {
            "exito": True,
            "year": year,
            "station": station,
            "total": 0,
            "type": "FeatureCollection",
            "features": []
        }

    features = features_to_geojson_from_db(rows)

    return {
        "exito": True,
        "year": year,
        "station": station,
        "total": len(features),
        "type": "FeatureCollection",
        "features": features
    }


@router.get("/mediciones/anio/{year}")
def endpoint_measurements_by_year_chunked(
    year: int,
    db: Session = Depends(get_db),
    block_size: int = 800
):

    all_features = []
    total = 0

    for chunk in db_service.stream_measurements_by_year(db, year, block_size):
        features = features_to_geojson_from_db(chunk)
        all_features.extend(features)
        total += len(features)

    return {
        "exito": True,
        "year": year,
        "block_size": block_size,
        "total_registros": total,
        "type": "FeatureCollection",
        "features": all_features
    }

@router.get("/mediciones/descargar-excel-estaciones/{year}")
def download_excel_by_year_grouped(
    year: int,
    db: Session = Depends(get_db)
):
    wb = Workbook()
    wb.remove(wb.active)  # Quitamos la hoja por defecto

    # Obtener todas las estaciones del año
    stations = db_service.get_stations_by_year(db, year)

    if not stations:
        raise HTTPException(404, "No hay datos para ese año")

    # Crear una hoja por estación
    for station in stations:
        ws = wb.create_sheet(title=str(station)[:31])  # Excel limita a 31 caracteres

        # Encabezados
        ws.append([
            "OBJECTID", "GLOBALID", "ESTACION", "FECHA", "PROFUNDIDAD",
            "TEMPERATURA", "SALINIDAD", "OXIGENO",
            "LONGITUD", "LATITUD"
        ])

        # Datos por bloques para la estación actual
        for chunk in db_service.stream_measurements_by_year_and_station(db, year, station):
            for r in chunk:
                ws.append([
                    r.objectid,
                    r.globalid,
                    r.estacion,
                    r.fecha.strftime("%Y-%m-%d %H:%M:%S") if r.fecha else None,
                    r.profundidad,
                    r.temperatura,
                    r.salinidad,
                    r.oxigeno,
                    r.longitud,
                    r.latitud,
                ])

    # Guardar en memoria
    output = BytesIO()
    wb.save(output)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename=iCEMAN_estaciones_{year}.xlsx"
        }
    )
