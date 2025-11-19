import requests
from app.core.config import settings

def fetch_all_arcgis_records(where: str = "1=1", batch_size: int | None = None):
    batch_size = batch_size or settings.arc_batch_size
    all_features = []
    offset = 0

    while True:
        params = {
            "where": where,
            "outFields": "*",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": batch_size
        }
        resp = requests.get(settings.full_external_api_url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features", [])
        if not features:
            break
        all_features.extend(features)
        if len(features) < batch_size:
            break
        offset += batch_size

    return all_features
