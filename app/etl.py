# script run by docker-compose loader service
import time
from datetime import datetime
from app.db import SessionLocal, engine, Base
from app.models import Medicion
from app.utils.arcgis_fetch_all import fetch_all_arcgis_records

# create tables if not exist
Base.metadata.create_all(bind=engine)

def ms_to_datetime(ms):
    if not ms:
        return None
    try:
        return datetime.utcfromtimestamp(ms/1000)
    except:
        return None

def upsert_records():
    features = fetch_all_arcgis_records(where="1=1")
    print(f"Fetched {len(features)} features")
    session = SessionLocal()
    try:
        for f in features:
            attrs = f.get("attributes", {}) or {}
            objid = attrs.get("OBJECTID")
            if objid is None:
                continue

            fecha_ms = attrs.get("Fecha")
            fecha_dt = ms_to_datetime(fecha_ms)

            row = session.query(Medicion).filter_by(objectid=objid).first()
            if row:
                row.globalid = attrs.get("GlobalID")
                row.estacion = attrs.get("Estacion")
                row.fecha_ms = fecha_ms
                row.fecha = fecha_dt
                row.longitud = attrs.get("Longitud")
                row.latitud = attrs.get("Latitud")
                row.profundidad = attrs.get("Profundidad")
                row.temperatura = attrs.get("Temperatura")
                row.salinidad = attrs.get("Salinidad")
                row.oxigeno = attrs.get("Oxigeno")
                row.created_date_ms = attrs.get("created_date")
                row.last_edited_date_ms = attrs.get("last_edited_date")
            else:
                row = Medicion(
                    objectid = objid,
                    globalid = attrs.get("GlobalID"),
                    estacion = attrs.get("Estacion"),
                    fecha_ms = fecha_ms,
                    fecha = fecha_dt,
                    longitud = attrs.get("Longitud"),
                    latitud = attrs.get("Latitud"),
                    profundidad = attrs.get("Profundidad"),
                    temperatura = attrs.get("Temperatura"),
                    salinidad = attrs.get("Salinidad"),
                    oxigeno = attrs.get("Oxigeno"),
                    created_date_ms = attrs.get("created_date"),
                    last_edited_date_ms = attrs.get("last_edited_date")
                )
                session.add(row)

        session.commit()
        print("ETL finished, committed")
    except Exception as e:
        session.rollback()
        print("ETL error:", e)
    finally:
        session.close()

if __name__ == "__main__":
    t0 = time.time()
    upsert_records()
    print("Elapsed:", time.time() - t0)
