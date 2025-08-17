"""helper functions used everywhere"""

import json
import time
from datetime import datetime
from pathlib import Path
import pandas as pd


def timestamp_now():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def safe_json_load(filepath):
    """load json, return {} on fail"""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"couldn't load json from {filepath}: {e}")
        return {}


def save_dataframe(df, name, timestamp=True):
    """save df as both csv and pickle"""
    from config import PROCESSED_DATA_DIR
    
    if timestamp:
        name = f"{name}_{timestamp_now()}"
    
    csv_path = PROCESSED_DATA_DIR / f"{name}.csv"
    df.to_csv(csv_path, index=False)
    
    pkl_path = PROCESSED_DATA_DIR / f"{name}.pkl"
    df.to_pickle(pkl_path)
    
    return csv_path, pkl_path


def retry_on_fail(func, max_attempts=3, delay=1):
    """retry func with exponential backoff"""
    for attempt in range(max_attempts):
        try:
            return func()
        except Exception as e:
            if attempt == max_attempts - 1:
                raise e
            print(f"attempt {attempt + 1} failed: {e}")
            print(f"retrying in {delay}s...")
            time.sleep(delay)
            delay *= 2


def clean_column_names(df):
    """lowercase cols, spaces to underscores"""
    df.columns = [col.lower().replace(' ', '_').replace('-', '_') 
                  for col in df.columns]
    return df