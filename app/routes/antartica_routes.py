from fastapi import APIRouter, Query, HTTPException, Depends
from sqlalchemy.orm import Session
from app.db import get_db
from app.services import db_service
from app.utils.geojson_converter import features_to_geojson_from_db

router = APIRouter(prefix="/antartica", tags=["Antártica"])

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
