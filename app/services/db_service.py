import requests
import httpx
import os
from datetime import datetime, timezone
from typing import List, Dict, Any
from app.db import SessionLocal
from fastapi import HTTPException
from app.models import Medicion
from sqlalchemy import select, func, extract, and_, distinct
from app.core.config import settings

def get_by_station(station: str, limit: int = 100, offset: int = 0):
    session = SessionLocal()
    try:
        q = select(Medicion).where(Medicion.estacion == station).limit(limit).offset(offset)
        rows = session.execute(q).scalars().all()
        return rows
    finally:
        session.close()

def get_all_stations(db):
    rows = (
        db.query(Medicion.estacion)
        .filter(Medicion.estacion.isnot(None))
        .distinct()
        .order_by(Medicion.estacion)
        .all()
    )
    return [r[0] for r in rows]  # extrae solo el string


def get_by_objectid(objectid: int):
    session = SessionLocal()
    try:
        return session.query(Medicion).filter(Medicion.objectid==objectid).first()
    finally:
        session.close()

def get_years(db):
    rows = (
        db.query(func.extract("year", Medicion.fecha).label("year"))
        .filter(Medicion.fecha.isnot(None))
        .distinct()
        .all()
    )

    years = []
    for r in rows:
        year_value = r.year

        # evitar None
        if year_value is None:
            continue

        # evitar años erróneos
        try:
            y = int(year_value)
            if 1900 <= y <= 2100:
                years.append(y)
        except:
            continue

    return sorted(set(years))


    # 2. Filtrar cualquier None que escape y convertir a entero
    years = []
    for r in rows:
        if r.year is not None:
            try:
                years.append(int(r.year))
            except:
                continue

    return sorted(list(set(years)))

def get_depths():
    session = SessionLocal()
    try:
        q = session.query(Medicion.profundidad).distinct().order_by(Medicion.profundidad)
        rows = q.all()
        return [r[0] for r in rows if r[0] is not None]
    finally:
        session.close()

def get_by_depth(db, depth: float):
    rows = (
        db.query(Medicion)
        .filter(Medicion.profundidad == depth)
        .order_by(Medicion.fecha.asc())
        .all()
    )
    return rows

def get_depths_by_station(db, station: str):
    rows = (
        db.query(Medicion.profundidad)
        .filter(Medicion.longitud != -99999)
        .filter(Medicion.latitud != -99999)
        .filter(Medicion.estacion == station)
        .filter(Medicion.profundidad.isnot(None))   # evita NULL
        .distinct()
        .order_by(Medicion.profundidad.asc())
        .all()
    )

    return [r[0] for r in rows]


def get_by_year_and_station(db, year: int, station: str):
    return (
        db.query(Medicion)
        .filter(Medicion.estacion == station)
        .filter(extract('year', Medicion.fecha) == year)
        .filter(Medicion.longitud != -99999)
        .filter(Medicion.latitud != -99999)
        .order_by(Medicion.fecha.asc())
        .all()
    )

def get_years_by_station(db, station: str):
    rows = (
        db.query(extract("year", Medicion.fecha).label("year"))
        .filter(Medicion.estacion == station)
        .filter(Medicion.fecha.isnot(None))
        .distinct()
        .all()
    )

    years = []
    for r in rows:
        try:
            y = int(r.year)
            if 1900 <= y <= 2100:
                years.append(y)
        except:
            continue

    return sorted(set(years))

def get_measurements_by_year(db, year: int, page: int, limit: int):
    offset = (page - 1) * limit

    query = (
        db.query(Medicion)
        .filter(func.extract('year', Medicion.fecha) == year)
        .filter(Medicion.longitud != -99999)
        .filter(Medicion.latitud != -99999)
    )

    total = query.count()

    rows = query.offset(offset).limit(limit).all()

    return total, rows


def get_first_records_by_year(db, year: int):
    rows = (
        db.query(Medicion)
        .filter(func.extract('year', Medicion.fecha) == year)
        .filter(Medicion.longitud != -99999)
        .filter(Medicion.latitud != -99999)
        .order_by(
            Medicion.estacion.asc(),
            Medicion.fecha.asc()
        )
        .distinct(Medicion.estacion)
        .all()
    )
    return rows

def get_records_by_year_and_station(db, year: int, station: str):
    rows = (
        db.query(Medicion)
        .filter(func.extract("year", Medicion.fecha) == year)
        .filter(Medicion.estacion == station)
        .filter(Medicion.longitud != -99999)
        .filter(Medicion.latitud != -99999)
        .order_by(Medicion.estacion.asc(), Medicion.fecha.asc())
        .all()
    )
    return rows

def stream_measurements_by_year(db, year: int, block_size: int = 800):
    offset = 0

    while True:
        rows = (
            db.query(Medicion)
            .filter(func.extract("year", Medicion.fecha) == year)
            .filter(Medicion.longitud != -99999)
            .filter(Medicion.latitud != -99999)
            .order_by(Medicion.id.asc())
            .offset(offset)
            .limit(block_size)
            .all()
        )

        if not rows:
            break

        yield rows

        offset += block_size

def get_stations_by_year(db, year: int):
    rows = (
        db.query(Medicion.estacion)
        .filter(func.extract("year", Medicion.fecha) == year)
        .filter(Medicion.estacion.isnot(None))
        .distinct()
        .order_by(Medicion.estacion.asc())
        .all()
    )
    return [r[0] for r in rows]


def stream_measurements_by_year_and_station(db, year: int, station: str, block_size: int = 500):
    offset = 0

    while True:
        rows = (
            db.query(Medicion)
            .filter(func.extract("year", Medicion.fecha) == year)
            .filter(Medicion.estacion == station)
            .filter(Medicion.longitud != -99999)
            .filter(Medicion.latitud != -99999)
            .order_by(Medicion.id.asc())
            .offset(offset)
            .limit(block_size)
            .all()
        )

        if not rows:
            break

        yield rows
        offset += block_size

async def get_unique_years() -> List[int]:
    """
    Obtiene los años únicos presentes en el campo 'Fecha' del servicio.
    """
    url = f"{settings.full_external_api_url}?where=1=1&outFields=Fecha&f=geojson"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                raise HTTPException(
                    status_code=502,
                    detail=f"Error del servicio ArcGIS: {data['error'].get('message', 'Desconocido')}"
                )

            unique_years = set()
            for feature in data.get("features", []):
                fecha_ms = feature.get("properties", {}).get("Fecha")
                if fecha_ms is not None:
                    try:
                        # Convertir milisegundos a año
                        year = datetime.utcfromtimestamp(fecha_ms / 1000).year
                        unique_years.add(year)
                    except (ValueError, OSError, TypeError):
                        continue  # valor inválido, lo ignoramos

            return sorted(unique_years)

    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Tiempo de espera agotado")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Error de red: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

async def get_unique_stations_by_year(year: int) -> List[Dict[str, Any]]:
    """
    Obtiene una única feature por estación (la primera encontrada)
    durante el año especificado, con geometría y propiedades completas.
    Usa la sintaxis DATE de ArcGIS para compatibilidad.
    """
    if year < 1900 or year > 2100:
        raise HTTPException(status_code=400, detail="Año fuera de rango válido")

    where_clause = f"Fecha >= DATE '{year}-01-01' AND Fecha < DATE '{year + 1}-01-01'"
    url = f"{settings.full_external_api_url}?where={where_clause}&outFields=*&f=geojson"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                msg = data["error"].get("message", "Error desconocido del servicio")
                raise HTTPException(status_code=502, detail=f"Error del servicio ArcGIS: {msg}")

            seen_stations = set()
            unique_features = []

            for feature in data.get("features", []):
                estacion = feature.get("properties", {}).get("Estacion")
                if estacion and estacion not in seen_stations:
                    unique_features.append(feature)
                    seen_stations.add(estacion)

            return unique_features

    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Tiempo de espera agotado")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Error de red: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")