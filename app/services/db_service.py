import requests
from app.db import SessionLocal
from fastapi import HTTPException
from app.models import Medicion
from sqlalchemy import select, func, extract
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

