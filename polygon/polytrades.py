import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
from pytz import timezone
from dotenv import load_dotenv
import numpy as np

load_dotenv()

# Configuration
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
PROXY = {
    'http': 'http://3.128.134.41:80',
    'https': 'http://3.128.134.41:80'
}
SYMBOL = 'AMD'

session = requests.Session()
session.proxies.update(PROXY)

def get_trades(date):
    print(f"\n=== Fetching trades for {date.date()} ===")
    url = f"http://3.128.134.41/v3/trades/{SYMBOL}"
    try:
        params = {
            'timestamp.gte': date.isoformat(),
            'timestamp.lte': (date + timedelta(hours=6.75)).isoformat(),
            'order': 'asc',
            'limit': 50000,
            'apiKey': POLYGON_API_KEY
        }
        all_results = []
        request_count = 0
        overall_start = time.time()
        
        while True:
            # Timeout safeguard
            if time.time() - overall_start > 300:  # 5 minute timeout
                print("Aborting due to 5 minute timeout")
                break
                
            request_count += 1
            print(f"Trade request #{request_count}")
            
            retries = 0
            while True:
                try:
                    response = session.get(url, params=params, timeout=30)
                    
                    # Handle rate limiting
                    if response.status_code == 429:
                        wait_time = 2 ** retries
                        print(f"Rate limited. Waiting {wait_time} seconds")
                        time.sleep(wait_time)
                        retries += 1
                        continue
                        
                    data = response.json()
                    break
                    
                except requests.exceptions.Timeout:
                    print("Timeout occurred, retrying...")
                    time.sleep(5)
            
            if response.status_code != 200 or 'results' not in data:
                print(f"Empty trades response for {date.date()}")
                break
                
            all_results.extend(data['results'])
            print(f"Added {len(data['results'])} trades (Total: {len(all_results)})")
            
            # Check for next page
            if 'next_url' in data:
                url = data['next_url'].replace('https://api.polygon.io', 'http://3.128.134.41')
                params = {'apiKey': POLYGON_API_KEY}  # Next URL already has other params
            else:
                break
                
        return pd.DataFrame(all_results) if all_results else pd.DataFrame()
        
    except Exception as e:
        print(f"Error getting trades: {str(e)}")
        return pd.DataFrame()

def process_trades(raw_trades):
    if raw_trades.empty:
        return pd.DataFrame()
    
    # Convert timestamps and sort (preserve original column)
    df = raw_trades.copy()
    df['participant_timestamp'] = pd.to_datetime(df['participant_timestamp'], utc=True).dt.tz_convert('America/New_York')
    df = df.sort_values('participant_timestamp').reset_index(drop=True)
    
    # Create price reference with explicit timestamp column
    prices = df[['participant_timestamp', 'price']].copy()
    
    # Calculate 15-minute lookback prices
    df['lookback_time'] = df['participant_timestamp'] - pd.Timedelta(minutes=15)
    merged = pd.merge_asof(
        df,
        prices.rename(columns={'price': 'prior_price'}),
        left_on='lookback_time',
        right_on='participant_timestamp',
        direction='backward'
    )
    
    # Explicitly keep original timestamp column
    merged = merged.rename(columns={
        'participant_timestamp_x': 'participant_timestamp',
        'participant_timestamp_y': 'lookback_timestamp'
    })
    
    # Calculate move_green flag
    merged['price_change_pct'] = (merged['price'] - merged['prior_price']) / merged['prior_price']
    merged['move_green'] = np.where(merged['price_change_pct'] > 0.005, 1, 0)
    
    # Ensure required columns exist
    return merged[[
        'id', 'sequence_number', 'price', 'size', 'move_green',
        'participant_timestamp', 'sip_timestamp',
        'exchange', 'tape', 'conditions'
    ]].dropna(subset=['move_green'])

def main():
    eastern = timezone('US/Eastern')
    all_processed = []
    
    # Loop through all days in January 2024
    for day in range(1, 32):  # January has 31 days
        try:
            # Create date for current day at 9:15am
            start_date = eastern.localize(datetime(2024, 1, day, 9, 15))
            print(f"\nProcessing {start_date.strftime('%Y-%m-%d')}")
            
            # Get trades for this day (9:15am to 4pm)
            raw_trades = get_trades(start_date)
            
            if not raw_trades.empty:
                # Process and collect
                processed = process_trades(raw_trades)
                all_processed.append(processed)
                print(f"Collected {len(processed)} trades for {start_date.date()}")
            else:
                print(f"No data for {start_date.strftime('%Y-%m-%d')}")
                
        except Exception as e:
            print(f"Error processing day {day}: {str(e)}")
    
    # Combine and save all data
    if all_processed:
        os.makedirs('csvoutputs', exist_ok=True)
        combined = pd.concat(all_processed, ignore_index=True)
        filename = f"csvoutputs/{SYMBOL}_all_trades_Jan2024.csv"
        combined.to_csv(filename, index=False)
        print(f"\nSaved {len(combined)} total trades to {filename}")
    else:
        print("\nNo data collected for any days in January 2024")

if __name__ == "__main__":
    main()