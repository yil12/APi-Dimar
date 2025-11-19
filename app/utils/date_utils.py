from datetime import datetime

def convertir_timestamp_ms_to_str(timestamp_ms):
    if not timestamp_ms:
        return None
    try:
        return datetime.utcfromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return None

def ms_to_datetime(timestamp_ms):
    if not timestamp_ms:
        return None
    try:
        return datetime.utcfromtimestamp(timestamp_ms / 1000)
    except:
        return None
