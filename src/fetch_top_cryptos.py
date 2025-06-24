import requests
import pandas as pd
import io
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time

load_dotenv()


def get_all_binance_symbols():
    """
    Fetches all available USDT trading pairs from Binance.

    Returns:
        set: Set of all valid USDT trading pairs
    """
    url = 'https://api.binance.com/api/v3/exchangeInfo'
    response = requests.get(url, timeout=10)

    if response.status_code != 200:
        print("Warning: Could not fetch Binance symbols")
        return set()

    data = response.json()
    usdt_symbols = set()

    for symbol_info in data['symbols']:
        symbol = symbol_info['symbol']
        if symbol.endswith('USDT') and symbol_info['status'] == 'TRADING':
            usdt_symbols.add(symbol)

    print(f"Loaded {len(usdt_symbols)} active USDT pairs from Binance")
    return usdt_symbols


def find_valid_symbol(base_symbol, valid_symbols):
    """
    Finds the correct Binance trading pair for a base symbol.

    Args:
        base_symbol (str): Base crypto symbol (e.g., 'HYPE', 'BTC')
        valid_symbols (set): Set of valid Binance symbols

    Returns:
        str or None: Valid trading pair or None if not found
    """
    # Try exact match first
    exact_match = base_symbol + 'USDT'
    if exact_match in valid_symbols:
        return exact_match

    # Try common variations
    variations = [
        base_symbol.upper() + 'USDT',
        'HYPER' + 'USDT' if base_symbol == 'HYPE' else None,
    ]

    # Try partial matches
    for symbol in valid_symbols:
        if symbol.startswith(base_symbol) and symbol.endswith('USDT'):
            variations.append(symbol)

    # Test each variation
    for variant in variations:
        if variant and variant in valid_symbols:
            if variant != exact_match:
                print(f"Found alternative: {base_symbol} -> {variant.replace('USDT', '')}")
            return variant

    return None


def get_top10_symbols(api_key=None, target_count=10, max_fetch=25):
    """
    Fetches top crypto symbols and validates them against Binance.

    Args:
        api_key (str, optional): CoinMarketCap API key
        target_count (int): Target number of valid symbols
        max_fetch (int): Maximum symbols to fetch from CMC

    Returns:
        list: List of valid USDT trading pairs
    """
    if api_key is None:
        api_key = os.getenv('COINMARKETCAP_API_KEY')

    # Get valid Binance symbols for validation
    valid_binance_symbols = get_all_binance_symbols()
    valid_symbols = []

    if api_key and valid_binance_symbols:
        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
        headers = {'X-CMC_PRO_API_KEY': api_key}
        params = {'start': '1', 'limit': str(max_fetch + 15), 'convert': 'USD', 'sort': 'market_cap'}

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()['data']

            stablecoins = ['USDT', 'USDC', 'BUSD', 'TUSD', 'DAI', 'FDUSD', 'USDE']
            excluded = ['WETH', 'WBTC', 'STETH']

            print("Validating CMC top cryptos against Binance...")

            for item in data:
                if len(valid_symbols) >= target_count:
                    break

                base_symbol = item['symbol']

                if base_symbol in stablecoins or base_symbol in excluded:
                    continue

                valid_pair = find_valid_symbol(base_symbol, valid_binance_symbols)

                if valid_pair:
                    valid_symbols.append(valid_pair)
                    print(f"{len(valid_symbols):2d}. {base_symbol} -> {valid_pair}")
                else:
                    print(f"No USDT pair found for {base_symbol}")

            if len(valid_symbols) >= target_count:
                print(f"Successfully validated {len(valid_symbols)} symbols from CMC")
                return valid_symbols[:target_count]
            else:
                print(f"Only found {len(valid_symbols)} valid symbols from CMC")
        else:
            print("CoinMarketCap API request failed")

    # Fallback: Use known symbols
    print("Using fallback symbols, validating against Binance...")

    fallback_bases = [
        'BTC', 'ETH', 'BNB', 'XRP', 'SOL', 'ADA', 'DOGE', 'AVAX', 
        'DOT', 'LINK', 'MATIC', 'LTC', 'UNI', 'ATOM', 'XLM', 
        'FIL', 'ETC', 'AAVE', 'ALGO', 'VET', 'TRX', 'SUI'
    ]

    for base in fallback_bases:
        if len(valid_symbols) >= target_count:
            break

        if valid_binance_symbols:
            valid_pair = find_valid_symbol(base, valid_binance_symbols)
            if valid_pair:
                valid_symbols.append(valid_pair)
        else:
            valid_symbols.append(base + 'USDT')

    print(f"Final symbol list: {[s.replace('USDT', '') for s in valid_symbols[:target_count]]}")
    return valid_symbols[:target_count]


def get_binance_vision_data(symbol, date=None, timeframe='1h'):
    """
    Fetches historical data from Binance Vision.

    Args:
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT')
        date (str, optional): Date in format 'YYYY-MM-DD'
        timeframe (str): Timeframe ('1h', '4h', '1d', etc.)

    Returns:
        pandas.DataFrame: DataFrame with close prices
    """
    if date is None:
        date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    base_url = "https://data.binance.vision/data/spot/daily/klines"
    url = f"{base_url}/{symbol}/{timeframe}/{symbol}-{timeframe}-{date}.zip"

    response = requests.get(url, timeout=30)

    if response.status_code != 200:
        return None

    df = pd.read_csv(io.BytesIO(response.content), compression='zip', header=None)

    df.columns = [
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'trades',
        'taker_base_vol', 'taker_quote_vol', 'ignore'
    ]

    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    df['close'] = df['close'].astype(float)

    return df[['close']]


def get_binance_klines(symbol, interval='1h', limit=168):
    """
    Fetches kline data from Binance API.

    Args:
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT')
        interval (str): Time interval
        limit (int): Number of klines to fetch

    Returns:
        pandas.DataFrame: DataFrame with close prices
    """
    url = f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}'
    response = requests.get(url, timeout=30)

    if response.status_code != 200:
        return None

    df = pd.DataFrame(response.json(), columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'trades',
        'taker_base_vol', 'taker_quote_vol', 'ignore'
    ])

    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    df['close'] = df['close'].astype(float)

    return df[['close']]


def get_multiple_cryptos_data(symbols=None, source='auto', days_back=7, timeframe='1h', 
                             api_key=None, target_count=10):
    """
    Fetches data for multiple cryptocurrency symbols.

    Args:
        symbols (list, optional): List of symbols. If None, auto-fetches top cryptos
        source (str): 'auto', 'api', or 'vision'
        days_back (int): Days of historical data (for API source)
        timeframe (str): Time interval
        api_key (str, optional): CoinMarketCap API key
        target_count (int): Target number of successful symbols

    Returns:
        pandas.DataFrame: Combined data for all symbols
    """
    if api_key is None:
        api_key = os.getenv('COINMARKETCAP_API_KEY')

    if symbols is None:
        print("Auto-fetching and validating top cryptocurrencies...")
        symbols = get_top10_symbols(api_key, target_count=target_count, max_fetch=target_count + 10)

    print(f"Fetching data for: {[s.replace('USDT', '') for s in symbols]}")
    print(f"Source: {source}, Timeframe: {timeframe}")

    all_data = []
    successful_symbols = []
    failed_symbols = []

    for i, symbol in enumerate(symbols):
        print(f"[{i+1}/{len(symbols)}] Fetching {symbol.replace('USDT', '')}...")
        data = None

        if source == 'auto':
            data = get_binance_vision_data(symbol, timeframe=timeframe)
            if data is None:
                print(f"Vision failed for {symbol}, trying API...")
                limit = min(days_back * 24, 1000) if timeframe == '1h' else days_back
                data = get_binance_klines(symbol, interval=timeframe, limit=limit)

        elif source == 'vision':
            data = get_binance_vision_data(symbol, timeframe=timeframe)

        elif source == 'api':
            limit = min(days_back * 24, 1000) if timeframe == '1h' else days_back
            data = get_binance_klines(symbol, interval=timeframe, limit=limit)

        if data is not None and len(data) > 0:
            data.columns = [symbol]
            all_data.append(data)
            successful_symbols.append(symbol)
            print(f"Success: {symbol} - {len(data)} data points")
        else:
            failed_symbols.append(symbol)
            print(f"Failed: {symbol}")

        time.sleep(0.1)

    if not all_data:
        raise ValueError("No data was successfully fetched for any symbol")

    combined_df = pd.concat(all_data, axis=1)
    combined_df = combined_df.dropna()

    print("Final Results:")
    print(f"Successful: {len(successful_symbols)} symbols")
    print(f"Failed: {len(failed_symbols)} symbols")
    print(f"Dataset shape: {combined_df.shape}")
    print(f"Date range: {combined_df.index[0]} to {combined_df.index[-1]}")
    print(f"Symbols: {[s.replace('USDT', '') for s in successful_symbols]}")

    return combined_df


def get_crypto_data_multi_day(symbols=None, start_date=None, end_date=None, 
                             api_key=None, target_count=10):
    """
    Fetches multiple days of data from Binance Vision.

    Args:
        symbols (list, optional): List of symbols
        start_date (str, optional): Start date 'YYYY-MM-DD'
        end_date (str, optional): End date 'YYYY-MM-DD'
        api_key (str, optional): CoinMarketCap API key
        target_count (int): Target number of symbols

    Returns:
        pandas.DataFrame: Multi-day combined data
    """
    if api_key is None:
        api_key = os.getenv('COINMARKETCAP_API_KEY')
    if symbols is None:
        print("Auto-fetching top cryptocurrencies...")
        symbols = get_top10_symbols(api_key, target_count=target_count, max_fetch=target_count + 10)

    if start_date is None:
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    print(f"Fetching data from {start_date} to {end_date}")
    print(f"Symbols: {[s.replace('USDT', '') for s in symbols]}")

    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    dates = pd.date_range(start, end, freq='D').strftime('%Y-%m-%d').tolist()

    all_symbol_data = {}
    successful_count = 0

    for symbol in symbols:
        if successful_count >= target_count:
            break

        symbol_data_list = []
        successful_days = 0

        for date in dates:
            data = get_binance_vision_data(symbol, date=date, timeframe='1h')
            if data is not None:
                symbol_data_list.append(data)
                successful_days += 1
            time.sleep(0.1)

        if symbol_data_list:
            symbol_combined = pd.concat(symbol_data_list)
            symbol_combined.columns = [symbol]
            all_symbol_data[symbol] = symbol_combined
            successful_count += 1
            print(f"Success: {symbol} - {len(symbol_combined)} data points from {successful_days} days")
        else:
            print(f"Failed: {symbol} - no data obtained")

    if not all_symbol_data:
        raise ValueError("No data was successfully fetched")

    final_df = pd.concat(all_symbol_data.values(), axis=1)
    final_df = final_df.dropna()

    print("Multi-day dataset complete:")
    print(f"Shape: {final_df.shape}")
    print(f"Date range: {final_df.index[0]} to {final_df.index[-1]}")

    return final_df
