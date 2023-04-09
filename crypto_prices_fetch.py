import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import numpy as np


class FetchData:
    def __init__(self) -> None:
        pass

    def fetch_crypto_prices(self, crypto_ticker: datetime, start_date: datetime, end_date: str) -> pd.DataFrame:
        """
        INPUT: crypto ticker, start date, end date
        OUTPUT: dataframe of crypto prices
        """
        df = yf.download(crypto_ticker, start_date, end_date, interval='1d')
        df = df.apply(lambda x: round(x, 6))
        df['Date'] = df.index.date
        # Timezone is UTC
        df.rename(columns={'Adj Close': 'Adj_Close'}, inplace=True)
        df['Time'] = df.index.time
        df['Prev_Close'] = df['Adj_Close'].shift(1)
        df['Simple_Return'] = round((df['Adj_Close'] - df['Prev_Close']) / df['Prev_Close'], 6)
        df['Log_Return'] = round(np.log(df['Adj_Close'] / df['Prev_Close']), 6)
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)
        print(df.head(30))
        return df
    
    def main(self, crypto_ticker: str):
        # crypto_ticker = "ETH-USD"
        format='%Y-%m-%d'
        # For interval 30m, we can only fetch data for 60 days
        end_date = datetime.today()
        start_date = end_date - timedelta(days=30)
        print(start_date, end_date)
        fetch = FetchData()
        df = fetch.fetch_crypto_prices(crypto_ticker, start_date, end_date)
        df.to_csv(f"/tmp/{crypto_ticker}-prices.csv", index=False)
        return f"{crypto_ticker}-prices.csv"
    
if __name__ == "__main__":
    crypto_ticker = "BTC-USD"
    format='%Y-%m-%d'
    # For interval 30m, we can only fetch data for 60 days
    end_date = datetime.today()
    start_date = end_date - timedelta(days=720)
    print(start_date, end_date)
    fetch = FetchData()
    df = fetch.fetch_crypto_prices(crypto_ticker, start_date, end_date)
    df.to_csv("/tmp/crypto_prices.csv", index=False)
