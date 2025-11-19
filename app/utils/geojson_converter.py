from app.utils.date_utils import convertir_timestamp_ms_to_str

def arcgis_to_geojson(features: list):
    geo_features = []

    for f in features:
        attrs = f.get("attributes", {}) or {}
        lon = attrs.get("Longitud")
        lat = attrs.get("Latitud")

        # Convertir fechas legibles
        if "Fecha" in attrs and attrs["Fecha"] is not None:
            attrs["Fecha"] = convertir_timestamp_ms_to_str(attrs["Fecha"])
        for fld in ("created_date", "last_edited_date"):
            if fld in attrs and attrs[fld]:
                attrs[fld] = convertir_timestamp_ms_to_str(attrs[fld])

        geometry = None
        if lon is not None and lat is not None:
            geometry = {
                "type": "Point",
                "coordinates": [lon, lat]
            }

        geo_features.append({
            "type": "Feature",
            "geometry": geometry,
            "properties": attrs
        })

    return {
        "type": "FeatureCollection",
        "features": geo_features
    }

# DB rows -> GeoJSON features
def features_to_geojson_from_db(rows):
    features = []
    for r in rows:
        props = {
            "objectid": r.objectid,
            "globalid": r.globalid,
            "estacion": r.estacion,
            "fecha": r.fecha.strftime("%Y-%m-%d %H:%M:%S") if r.fecha else None,
            "profundidad": r.profundidad,
            "temperatura": r.temperatura,
            "salinidad": r.salinidad,
            "oxigeno": r.oxigeno,
            "created_date_ms": r.created_date_ms,
            "last_edited_date_ms": r.last_edited_date_ms
        }
        geometry = None
        if r.longitud is not None and r.latitud is not None:
            geometry = {"type": "Point", "coordinates": [r.longitud, r.latitud]}
        features.append({"type": "Feature", "geometry": geometry, "properties": props})
    return features
