"""
WIP: fetch weather data for nyc
"""

import requests
import json
from datetime import datetime
from pathlib import Path
import pandas as pd

import sys
from pathlib import Path
# add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import OPENWEATHER_API_KEY, NYC_LAT, NYC_LON, WEATHER_API_BASE, RAW_DATA_DIR
from utils.logger import get_logger
from utils.helpers import retry_on_fail, timestamp_now

logger = get_logger("weather_fetcher")


class WeatherFetcher:
    """fetches weather data from openweathermap"""
    
    def __init__(self):
        self.api_key = OPENWEATHER_API_KEY
        self.base_url = WEATHER_API_BASE
        self.save_dir = RAW_DATA_DIR / "weather"
        self.save_dir.mkdir(exist_ok=True)
        
        if not self.api_key:
            logger.warning("no openweather api key found - get one at openweathermap.org/api")
    
    def fetch_current_weather(self):
        """get current weather for nyc"""
        if not self.api_key:
            logger.error("missing api key")
            return None
            
        url = f"{self.base_url}/weather"
        params = {
            'lat': NYC_LAT,
            'lon': NYC_LON,
            'appid': self.api_key,
            'units': 'imperial'  # fahrenheit for nyc
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"failed to fetch weather: {e}")
            return None
    
    def fetch_forecast(self, hours=24):
        """get hourly forecast"""
        if not self.api_key:
            return None
            
        url = f"{self.base_url}/forecast"
        params = {
            'lat': NYC_LAT,
            'lon': NYC_LON,
            'appid': self.api_key,
            'units': 'imperial',
            'cnt': hours // 3  # api returns 3-hour intervals
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"failed to fetch forecast: {e}")
            return None
    
    def parse_weather_data(self, current, forecast=None):
        """extract the useful bits"""
        if not current:
            return None
            
        # current conditions
        data = {
            'timestamp': datetime.now().isoformat(),
            'temp': current.get('main', {}).get('temp'),
            'feels_like': current.get('main', {}).get('feels_like'),
            'humidity': current.get('main', {}).get('humidity'),
            'pressure': current.get('main', {}).get('pressure'),
            'description': current.get('weather', [{}])[0].get('description', ''),
            'wind_speed': current.get('wind', {}).get('speed'),
            'clouds': current.get('clouds', {}).get('all'),
            'visibility': current.get('visibility'),
        }
        
        # add rain if present
        if 'rain' in current:
            data['rain_1h'] = current.get('rain', {}).get('1h', 0)
        
        # add snow if present (important for nyc)
        if 'snow' in current:
            data['snow_1h'] = current.get('snow', {}).get('1h', 0)
            
        return data
    
    def fetch_and_save(self):
        """main fetch / save flow"""
        logger.info("fetching nyc weather...")
        
        # get current weather
        current = retry_on_fail(
            lambda: self.fetch_current_weather(),
            max_attempts=2
        )
        
        # get forecast
        forecast = retry_on_fail(
            lambda: self.fetch_forecast(),
            max_attempts=2
        )
        
        if not current:
            logger.warning("no weather data fetched")
            return None
        
        # parse
        weather_data = self.parse_weather_data(current, forecast)
        
        # save raw responses
        timestamp = timestamp_now()
        
        if current:
            current_path = self.save_dir / f"current_{timestamp}.json"
            with open(current_path, 'w') as f:
                json.dump(current, f, indent=2)
            logger.info(f"saved current weather to {current_path}")
        
        if forecast:
            forecast_path = self.save_dir / f"forecast_{timestamp}.json"
            with open(forecast_path, 'w') as f:
                json.dump(forecast, f, indent=2)
            logger.info(f"saved forecast to {forecast_path}")
        
        # save parsed data
        if weather_data:
            parsed_path = self.save_dir / f"parsed_{timestamp}.json"
            with open(parsed_path, 'w') as f:
                json.dump(weather_data, f, indent=2)
            
            # log current conditions
            logger.info(f"current: {weather_data['temp']}°F, {weather_data['description']}")
            logger.info(f"feels like: {weather_data['feels_like']}°F")
            
            if weather_data.get('rain_1h'):
                logger.info(f"rain: {weather_data['rain_1h']}mm in last hour")
            if weather_data.get('snow_1h'):
                logger.info(f"snow: {weather_data['snow_1h']}mm in last hour")
        
        return weather_data


if __name__ == "__main__":
    # test run
    fetcher = WeatherFetcher()
    data = fetcher.fetch_and_save()
    if data:
        print(f"\ncurrent weather in nyc:")
        print(f"  {data['temp']}°F - {data['description']}")
        print(f"  feels like {data['feels_like']}°F")
        print(f"  humidity: {data['humidity']}%")
    else:
        print("couldn't fetch weather - check your api key in .env")