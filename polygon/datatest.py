import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
from pytz import timezone
import pytz
from dotenv import load_dotenv

load_dotenv()

# Configuration
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
POLYGON_API_URL = os.getenv('POLYGON_API_URL')
PROXY = {
    'http': 'http://3.128.134.41:80',
    'https': 'http://3.128.134.41:80'
}
SYMBOL = 'AMD'
INTERVAL_MINUTES = 15
TRADING_DAYS = 1

session = requests.Session()
session.proxies.update(PROXY)

def get_trades(date):
    overall_start = time.time()
    print(f"\n=== Fetching trades for {date.date()} ===")
    request_count = 0
    url = f"http://3.128.134.41/v3/trades/{SYMBOL}"
    try:
        date_str = date.strftime('%Y-%m-%d')
        params = {
            'date': date_str,
            'order': 'asc',
            'limit': 50000,
            'apiKey': POLYGON_API_KEY
        }
        all_results = []
        
        while True:
            if time.time() - overall_start > 300:  # 5 minute timeout
                print("Aborting due to 5 minute timeout")
                break
            request_count += 1
            print(f"Trade request #{request_count}")
            retries = 0
            while True:
                try:
                    response = session.get(url, params=params, timeout=30)
                    if response.status_code == 429:
                        wait_time = 2 ** retries
                        print(f"Rate limited. Waiting {wait_time} seconds")
                        time.sleep(wait_time)
                        retries += 1
                        continue
                    response_json = response.json()
                    print(response_json)
                    print(response.status_code)
                    
                    if response.status_code != 200 or 'results' not in response_json:
                        print(f"Empty trades response for {date}")
                        break
                        
                    all_results.extend(response_json['results'])
                    
                    # Check for next page
                    if 'next_url' in response_json:
                        url = response_json['next_url'].replace('https://api.polygon.io', 'http://3.128.134.41')
                        params = {"limit": 50000, "apiKey": POLYGON_API_KEY}
                    else:
                        break
                    
                except requests.exceptions.Timeout:
                    print("Timeout occurred, retrying...")
                    time.sleep(5)
                
        return pd.DataFrame(all_results) if all_results else pd.DataFrame(columns=['participant_timestamp', 'price', 'size', 'exchange', 'condition'])
    except Exception as e:
        print(f"Error getting trades: {str(e)}")
        return pd.DataFrame(columns=['participant_timestamp', 'price', 'size', 'exchange', 'condition'])

def get_quotes(date):
    url = f"http://3.128.134.41/v3/quotes/{SYMBOL}"
    try:
        date_str = date.strftime('%Y-%m-%d')
        params = {
            'date': date_str,
            'order': 'asc',
            'limit': 50000,
            'apiKey': POLYGON_API_KEY
        }
        all_results = []
        
        while True:
            print(f"Making request to {url}")
            retries = 0
            while True:
                try:
                    response = session.get(url, params=params, timeout=30)
                    if response.status_code == 429:
                        wait_time = 2 ** retries
                        print(f"Rate limited. Waiting {wait_time} seconds")
                        time.sleep(wait_time)
                        retries += 1
                        continue
                    response_json = response.json()
                    
                    if response.status_code != 200 or 'results' not in response_json:
                        print(f"Empty quotes response for {date}")
                        break
                        
                    all_results.extend(response_json['results'])
                    
                    # Check for next page
                    if 'next_url' in response_json:
                        url = response_json['next_url'].replace('https://api.polygon.io', 'http://3.128.134.41')
                        params = {"limit": 50000, "apiKey": POLYGON_API_KEY}
                        print(f"Found next page: {response_json['next_url']}")
                    else:
                        break
                    
                except requests.exceptions.Timeout:
                    print("Timeout occurred, retrying...")
                    time.sleep(5)
                
        return pd.DataFrame(all_results) if all_results else pd.DataFrame(columns=['participant_timestamp', 'ask_price', 'bid_price', 'ask_size', 'bid_size'])
    except Exception as e:
        print(f"Error getting quotes: {str(e)}")
        return pd.DataFrame(columns=['participant_timestamp', 'ask_price', 'bid_price', 'ask_size', 'bid_size'])

def resample_data(df, column_map):
    if df.empty:
        print("Empty dataframe received for resampling")
        return pd.DataFrame(columns=column_map.values())
    
    print(f"Resampling {len(df)} raw data points")
    # Add check for minimum data points
    if len(df) < 26:
        print(f"Warning: Only {len(df)} data points for 15m window")
    
    try:
        # Handle empty dataframes
        if df.empty:
            return pd.DataFrame(columns=column_map.values())
            
        # Force timezone conversion with error handling
        df['timestamp'] = pd.to_datetime(df['participant_timestamp'], errors='coerce', utc=True)
        df = df.dropna(subset=['timestamp']).set_index('timestamp').tz_convert('America/New_York')
        
        # Handle empty dataframe after filtering
        if df.empty:
            return pd.DataFrame(columns=column_map.values())
            
        resampled = df.resample(f'{INTERVAL_MINUTES}T').agg({
            'price': ['first', 'last', 'max', 'min'],
            'size': 'sum'
        })
        resampled.columns = ['_'.join(col).strip() for col in resampled.columns.values]
        return resampled.rename(columns=column_map)
    except Exception as e:
        print(f"Resampling error: {str(e)}")
        return pd.DataFrame(columns=column_map.values())

def get_technicals(df):
    # Ensure we have valid timestamps
    if df.empty or not isinstance(df.index, pd.DatetimeIndex):
        return pd.DataFrame()
    
    # Create time filter parameters
    start_time = df.index.min().isoformat()
    end_time = df.index.max().isoformat()
    
    technicals = pd.DataFrame(index=df.index).sort_index()
    
    # SMA (40 period) - updated parameters
    sma_params = {
        'timestamp.gte': start_time,
        'timestamp.lte': end_time,
        'window': 40,
        'timespan': 'minute',
        'adjusted': 'true',
        'apiKey': POLYGON_API_KEY
    }
    sma_data = session.get(f"http://3.128.134.41/v1/indicators/sma/{SYMBOL}", params=sma_params).json()
    technicals['sma_40'] = [r['value'] for r in sma_data['results']['values']]

    # MACD (12/26/9)
    macd_url = f"http://3.128.134.41/v1/indicators/macd/{SYMBOL}"
    macd_params = {
        'short_window': 12,
        'long_window': 26,
        'signal_window': 9,
        'timespan': 'minute',
        'apiKey': POLYGON_API_KEY
    }
    macd_data = session.get(macd_url, params=macd_params).json()
    technicals['macd'] = [r['value'] for r in macd_data['results']['values']]

    # RSI (14 period)
    rsi_url = f"http://3.128.134.41/v1/indicators/rsi/{SYMBOL}"
    rsi_params = {
        'window': 14,
        'timespan': 'minute',
        'apiKey': POLYGON_API_KEY
    }
    rsi_data = session.get(rsi_url, params=rsi_params).json()
    technicals['rsi_14'] = [r['value'] for r in rsi_data['results']['values']]

    return technicals

def get_financials():
    try:
        url = "http://3.128.134.41/vX/reference/financials"
        params = {
            'ticker': SYMBOL,
            'timeframe': 'quarterly',
            'order': 'desc',
            'limit': 1,
            'apiKey': POLYGON_API_KEY
        }
        response = session.get(url, params=params)
        if response.status_code != 200 or 'results' not in response.json():
            return pd.DataFrame([{'equity': 'N/A', 'assets': 'N/A', 'liabilities': 'N/A', 'revenue': 'N/A', 'earnings': 'N/A'}])
            
        financials = pd.DataFrame(response.json()['results'])
        return financials[['equity', 'assets', 'liabilities', 'revenue', 'earnings']]
    except Exception as e:
        print(f"Error getting financials: {str(e)}")
        return pd.DataFrame([{'equity': 'N/A', 'assets': 'N/A', 'liabilities': 'N/A', 'revenue': 'N/A', 'earnings': 'N/A'}])

def calculate_custom_metrics(df):
    # Add missing columns if they don't exist
    for col in ['bid_size', 'ask_size', 'bid_exchange', 'ask_exchange', 'conditions']:
        if col not in df.columns:
            df[col] = 0  # or appropriate default value
            
    # Safe calculations with error handling
    try:
        df['bid_ask_ratio'] = df['bid_size'] / df['ask_size'].replace(0, 1)
        df['bid_ask_exchange_ratio'] = df['bid_exchange'].value_counts() / \
                                      (df['ask_exchange'].count() + df['bid_exchange'].count() + 1e-6)
        condition_counts = df['conditions'].explode().value_counts()
        df['conditions_ratio'] = condition_counts / (condition_counts.sum() + 1e-6)

        # Trades metrics
        df['trades_exchange_ratio'] = df['exchange'].value_counts() / df['exchange'].count()
        trade_condition_counts = df['condition'].explode().value_counts()
        df['trades_conditions_ratio'] = trade_condition_counts / trade_condition_counts.sum()

        # Indicator metrics
        df['vwap_pos'] = df['price'].mean() / df['vwap']
        df['vol_spike'] = df['volume'] / df['volume'].rolling(4).mean()

        # Original metrics
        df['price_change'] = df['close_price'] - df['open_price']
        df['volume_oscillator'] = df['total_volume'] / df['total_volume'].rolling(4).mean()
        
    except Exception as e:
        print(f"Error in custom metrics: {str(e)}")
        
    return df.dropna()

def main():
    # Hardcoded specific date range
    eastern = timezone('US/Eastern')
    start_date = eastern.localize(datetime(2025, 2, 3, 9, 30))  # 2025-02-03 09:30 EST
    end_date = eastern.localize(datetime(2025, 2, 3, 16, 0))    # 2025-02-03 16:00 EST
    
    master_df = pd.DataFrame()
    
    # Create 15-minute intervals for the specific day
    current_time = start_date
    while current_time <= end_date:
        print(f"\nProcessing interval: {current_time.strftime('%Y-%m-%d %H:%M')}")
        
        try:
            # Get raw data for this 15-minute window
            trades = get_trades(current_time)
            quotes = get_quotes(current_time)
            
            # Resample to 15-minute intervals (though already windowed)
            trades_15min = resample_data(trades, {
                'price_first': 'open_price',
                'price_last': 'close_price',
                'price_max': 'high_price',
                'price_min': 'low_price',
                'size_sum': 'total_volume'
            })
            
            quotes_15min = resample_data(quotes, {
                'price_first': 'bid_open',
                'price_last': 'ask_close',
                'price_max': 'ask_high',
                'price_min': 'bid_low',
                'size_sum': 'quote_volume'
            })
            
            # Merge and accumulate data
            merged = pd.merge(trades_15min, quotes_15min, 
                            left_index=True, right_index=True, how='outer')
            master_df = pd.concat([master_df, merged])
            
            time.sleep(12)  # Maintain rate limiting
            
        except Exception as e:
            print(f"Error processing {current_time}: {str(e)}")
        
        # Move to next 15-minute interval
        current_time += timedelta(minutes=15)

    # Add technical indicators
    technicals = get_technicals(master_df)
    master_df = pd.merge(master_df, technicals, left_index=True, right_index=True)
    
    # Add financials
    financials = get_financials()
    master_df = master_df.assign(**financials.iloc[0].to_dict())
    
    # Clean and finalize
    master_df = calculate_custom_metrics(master_df)
    master_df.to_csv(f'csvoutputs/{SYMBOL}_master_dataset_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')

if __name__ == "__main__":
    if not os.path.exists('csvoutputs'):
        os.makedirs('csvoutputs')
    main()

# Temporary test code
response = session.get("http://3.128.134.41/v3/marketstatus", timeout=5)
print(f"Proxy response time: {response.elapsed.total_seconds()}s")


