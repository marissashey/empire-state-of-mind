"""project configs: handling environment variables / shared settings"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
REPORTS_DIR = BASE_DIR / "reports"

# ensure dirs exist
for dir_path in [RAW_DATA_DIR, PROCESSED_DATA_DIR, REPORTS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# api keys
MTA_API_KEY = os.getenv("MTA_API_KEY", "")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
NYC_APP_TOKEN = os.getenv("NYC_APP_TOKEN")

# database
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///empire_state.db")

# endpoints
MTA_SUBWAY_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"
NYC_OPEN_DATA_BASE = "https://data.cityofnewyork.us/resource"
WEATHER_API_BASE = "https://api.openweathermap.org/data/2.5"

# manhattan center
NYC_LAT = 40.7580
NYC_LON = -73.9855

# refresh intervals (seconds)
REFRESH_INTERVALS = {
    "mta": 60,  # 1m
    "weather": 600,  # 10m
    "events": 3600,  # 1h
    "crime": 86400,  # 1d
}