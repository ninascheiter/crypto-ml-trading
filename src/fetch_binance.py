import ccxt
import pandas as pd
import os

# get data for specific timeframe # each line ia a candle in the respective timeframe, only 1000 requests in one
# if more data is needed, load in a loop
# %TODO load more data in loop for cross cycle analysis
def fetch_and_save(symbol = "BTC/USDT", timeframe="1h", since="2021-01-01T00:00:00Z"):
    exchange = ccxt.binance() # CCXT = CryptoCurrency eXchange Trading library; from binance
    
    ohlcv = exchange.fetch_ohlcv(
        symbol,
        timeframe=timeframe,
        since=exchange.parse8601(since)
    ) # Open High Low Close Volume - load historic data
    
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
   
    os.makedirs("data", exist_ok=True) #to avoid error if directory already exists
    filename = f"data/{symbol.replace('/', '')}_{timeframe}.parquet"
    df.to_parquet(filename, index=False) #indexes not needed for candles, have time
    
    print(f"Saved: {filename} with {len(df)} lines")
    return df

if __name__ == "__main__":
    df=fetch_and_save()
    print(df.head())