import json
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
RAW_WEATHER = ROOT / "data" / "raw" / "weather"
RAW_FLIGHTS = ROOT / "data" / "raw" / "flights"
DOCS_LATEST = ROOT / "docs" / "data" / "latest.json"

# Cyprus-ish defaults (adjust later)
LAT, LON = 34.9, 33.6

# Small bbox to reduce OpenSky API credit usage
LAMIN, LOMIN, LAMAX, LOMAX = 34.5, 32.7, 35.6, 34.2

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
OPENSKY_URL = "https://opensky-network.org/api/states/all"

UA = "Weather-Flights-Predictions/1.0 (raw-storage)"


def now_id():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_dirs():
    RAW_WEATHER.mkdir(parents=True, exist_ok=True)
    RAW_FLIGHTS.mkdir(parents=True, exist_ok=True)
    DOCS_LATEST.parent.mkdir(parents=True, exist_ok=True)


def fetch(url: str, params: dict):
    r = requests.get(url, params=params, timeout=30, headers={"User-Agent": UA})
    body_text = r.text
    try:
        payload = r.json()
    except Exception:
        payload = None
    return r.status_code, r.url, body_text, payload


def write_json(path: Path, obj: dict):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def prune(folder: Path, keep_last: int = 200):
    files = sorted(folder.glob("*.json"), key=lambda p: p.name)
    for p in files[:-keep_last]:
        p.unlink(missing_ok=True)


def main():
    ensure_dirs()
    rid = now_id()
    retrieved_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    # ---- Open-Meteo (forecast) ----
    meteo_params = {
        "latitude": LAT,
        "longitude": LON,
        "hourly": "temperature_2m,precipitation,wind_speed_10m",
        "timezone": "UTC",
    }
    w_status, w_final_url, w_body, w_payload = fetch(OPEN_METEO_URL, meteo_params)

    w_raw = {
        "retrieved_at_utc": retrieved_at_utc,
        "source": "open-meteo",
        "final_url": w_final_url,
        "http_status": w_status,
        "payload": w_payload,  # exact JSON payload
    }
    write_json(RAW_WEATHER / f"{rid}.json", w_raw)

    # ---- OpenSky (live state vectors) ----
    sky_params = {"lamin": LAMIN, "lomin": LOMIN, "lamax": LAMAX, "lomax": LOMAX}
    f_status, f_final_url, f_body, f_payload = fetch(OPENSKY_URL, sky_params)

    f_raw = {
        "retrieved_at_utc": retrieved_at_utc,
        "source": "opensky",
        "final_url": f_final_url,
        "http_status": f_status,
        "payload": f_payload,  # exact JSON payload (or None if parse failed)
        "raw_text_fallback": None if f_payload is not None else f_body,
    }
    write_json(RAW_FLIGHTS / f"{rid}.json", f_raw)

    # ---- Curated snapshot for the website ----
    # Weather: next 24 hours (if present)
    weather_next_24h = None
    if isinstance(w_payload, dict) and isinstance(w_payload.get("hourly"), dict):
        h = w_payload["hourly"]
        weather_next_24h = {
            "time": (h.get("time") or [])[:24],
            "temperature_2m": (h.get("temperature_2m") or [])[:24],
            "precipitation": (h.get("precipitation") or [])[:24],
            "wind_speed_10m": (h.get("wind_speed_10m") or [])[:24],
        }

    # Flights: aircraft count in bbox (if present)
    aircraft_count = None
    if isinstance(f_payload, dict) and isinstance(f_payload.get("states"), list):
        aircraft_count = len(f_payload["states"])

    latest = {
        "generated_at_utc": retrieved_at_utc,
        "location": {"lat": LAT, "lon": LON},
        "opensky_bbox": {"lamin": LAMIN, "lomin": LOMIN, "lamax": LAMAX, "lomax": LOMAX},
        "open_meteo_status": w_status,
        "opensky_status": f_status,
        "aircraft_count_in_bbox": aircraft_count,
        "weather_next_24h": weather_next_24h,
    }
    write_json(DOCS_LATEST, latest)

    # Keep repo from growing forever
    prune(RAW_WEATHER, keep_last=200)
    prune(RAW_FLIGHTS, keep_last=200)

    print("OK", rid)


if __name__ == "__main__":
    main()