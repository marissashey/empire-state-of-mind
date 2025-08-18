"""
WIP: fetch mta performance data from nyc open data
"""

import requests
import json
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import NYC_APP_TOKEN, RAW_DATA_DIR
from utils.logger import get_logger
from utils.helpers import retry_on_fail, timestamp_now, clean_column_names

logger = get_logger("mta_fetcher")


class MTAFetcher:
    """fetches subway performance metrics from nyc open data"""
    
    def __init__(self):
        self.base_url = "https://data.ny.gov/resource"
        self.save_dir = RAW_DATA_DIR / "mta"
        self.save_dir.mkdir(exist_ok=True)
        
        # dataset ids from nyc open data
        self.datasets = {
            'terminal_otp': 'f6rf-2a3t',  # on-time performance by line
            'mean_distance_failures': 'e7fs-9m87',  # reliability metric
            'additional_platform_time': 'i9wp-a4ja',  # extra wait time by line
        }
        
        # nyc subway lines for reference
        self.lines = ['1', '2', '3', '4', '5', '6', '6X', '7', '7X', 
                     'A', 'B', 'C', 'D', 'E', 'F', 'G', 'J', 'L', 'M', 
                     'N', 'Q', 'R', 'S', 'W', 'Z']
        
    def fetch_dataset(self, dataset_key, limit=1000, where_clause=None):
        """fetch data from nyc open data using socrata api"""
        if dataset_key not in self.datasets:
            logger.error(f"unknown dataset: {dataset_key}")
            return None
            
        dataset_id = self.datasets[dataset_key]
        url = f"{self.base_url}/{dataset_id}.json"
        
        params = {
            '$limit': limit,
            '$order': ':id',
        }
        
        # add app token if available (higher rate limits)
        if NYC_APP_TOKEN:
            params['$$app_token'] = NYC_APP_TOKEN
            
        # add where clause for filtering
        if where_clause:
            params['$where'] = where_clause
            
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"failed to fetch {dataset_key}: {e}")
            return None
    
    def fetch_terminal_otp(self):
        """fetch on-time performance by line"""
        logger.info("fetching terminal on-time performance...")
        
        # get recent data (last month if available)
        data = retry_on_fail(
            lambda: self.fetch_dataset('terminal_otp', limit=2000),
            max_attempts=2
        )
        
        if not data:
            return None
            
        # convert to dataframe
        df = pd.DataFrame(data)
        
        if df.empty:
            logger.warning("no otp data received")
            return None
            
        # clean columns
        df = clean_column_names(df)
        
        # save
        save_path = self.save_dir / f"terminal_otp_{timestamp_now()}.csv"
        df.to_csv(save_path, index=False)
        logger.info(f"saved {len(df)} otp records to {save_path}")
        
        # summarize by line if data has line column
        if 'line' in df.columns:
            line_summary = df.groupby('line').agg({
                'otp': 'mean'  # assuming there's an otp column
            }).round(2)
            
            logger.info("on-time performance by line:")
            for line, otp in line_summary.iterrows():
                logger.info(f"  {line}: {otp['otp']}%")
                
        return df
    
    def fetch_platform_wait_times(self):
        """fetch additional platform time (delays) by line"""
        logger.info("fetching platform wait times...")
        
        data = retry_on_fail(
            lambda: self.fetch_dataset('additional_platform_time', limit=2000),
            max_attempts=2
        )
        
        if not data:
            return None
            
        df = pd.DataFrame(data)
        
        if df.empty:
            logger.warning("no platform time data received")
            return None
            
        df = clean_column_names(df)
        
        # save raw
        save_path = self.save_dir / f"platform_times_{timestamp_now()}.csv"
        df.to_csv(save_path, index=False)
        logger.info(f"saved {len(df)} platform time records to {save_path}")
        
        # analyze if we have the right columns
        # looking for columns like 'line', 'additional_time', etc
        logger.info(f"columns found: {list(df.columns)}")
        
        return df
    
    def fetch_all(self):
        """fetch all mta datasets"""
        logger.info("starting mta data fetch...")
        
        results = {}
        
        # terminal on-time performance
        otp_data = self.fetch_terminal_otp()
        if otp_data is not None:
            results['terminal_otp'] = otp_data
            
        # platform wait times
        wait_data = self.fetch_platform_wait_times()
        if wait_data is not None:
            results['platform_times'] = wait_data
            
        # create summary
        if results:
            summary_path = self.save_dir / f"fetch_summary_{timestamp_now()}.json"
            summary = {
                'timestamp': datetime.now().isoformat(),
                'datasets_fetched': list(results.keys()),
                'record_counts': {k: len(v) for k, v in results.items()}
            }
            
            with open(summary_path, 'w') as f:
                json.dump(summary, f, indent=2)
                
            logger.info(f"fetch complete. summary saved to {summary_path}")
            
        return results


if __name__ == "__main__":
    fetcher = MTAFetcher()
    
    # test individual fetches
    print("\ntesting terminal otp fetch...")
    otp = fetcher.fetch_terminal_otp()
    if otp is not None:
        print(f"  fetched {len(otp)} records")
        print(f"  columns: {list(otp.columns)[:5]}...")  # show first 5 cols
        
    print("\ntesting platform wait times...")
    waits = fetcher.fetch_platform_wait_times()
    if waits is not None:
        print(f"  fetched {len(waits)} records")
        print(f"  columns: {list(waits.columns)[:5]}...")
        
    # or fetch everything
    # print("\nfetching all datasets...")
    # results = fetcher.fetch_all()
    # print(f"fetched {len(results)} datasets")