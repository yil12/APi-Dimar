from sqlalchemy import Column, Integer, String, Float, BigInteger, DateTime
from app.db import Base

class Medicion(Base):
    __tablename__ = "mediciones"

    id = Column(Integer, primary_key=True, autoincrement=True)
    objectid = Column(Integer, unique=True, index=True, nullable=False)    # OBJECTID ArcGIS
    globalid = Column(String(100), index=True)
    estacion = Column(String(255), index=True)
    fecha_ms = Column(BigInteger)  # timestamp ms from ArcGIS
    fecha = Column(DateTime)       # converted datetime (UTC)
    longitud = Column(Float)
    latitud = Column(Float)
    profundidad = Column(Float, index=True)
    temperatura = Column(Float)
    salinidad = Column(Float)
    oxigeno = Column(Float)
    created_date_ms = Column(BigInteger)
    last_edited_date_ms = Column(BigInteger)
