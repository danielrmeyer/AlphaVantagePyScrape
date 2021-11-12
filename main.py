import requests
import configparser
import pandas as pd
import json

from requests.api import head

config = configparser.ConfigParser()
config.read("config.cfg")
accesskey = config["alphavantage"]["accesskey"]


def query_api(function, symbol, interval, outputsize, accesskey):
    url = f"https://www.alphavantage.co/query?function={function}&symbol={symbol}&interval={interval}&apikey=accesskey"

    r = requests.get(url)
    data = r.json()
    return data


def fetch_time_series_intraday(
    symbol, interval, adjusted="true", outputsize="compact", datatype="json"
):

    url = f"https://www.alphavantage.co/query?function={function}&symbol={symbol}&interval={interval}&apikey=accesskey"


# def fetch_time_series_intraday(symbol, interval, outputsize):
#     args = ("TIME_SERIES_INTRADAY", symbol, interval, outputsize, accesskey)

#     data = query_api(*args)

#     df, md = transform(data)

#     return df, md


# def fetch_time_series_daily(symbol, interval, outputsize):
#     args = ("TIME_SERIES_DAILY", symbol, outputsize, accesskey)

#     data = query_api(*args)

#     df, md = transform(data)

#     return df, md


def write_json_data(data):
    with open("data.json", "w") as output_fp:
        output_fp.write(json.dumps(data))


def transform(data):
    meta_data = data["Meta Data"]
    ts = data["Time Series (5min)"]

    columns = ["datetime", "open", "high", "low", "close", "volume"]
    sanitized_data = []

    for t in ts:
        price = ts[t]
        open = price["1. open"]
        high = price["2. high"]
        low = price["3. low"]
        close = price["4. close"]
        volume = price["5. volume"]

    sanitized_data.append([t, open, high, low, close, volume])
    df = pd.DataFrame(sanitized_data, columns=columns)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["open"] = pd.to_numeric(df["open"])
    df["high"] = pd.to_numeric(df["high"])
    df["low"] = pd.to_numeric(df["low"])
    df["close"] = pd.to_numeric(df["close"])
    df["volume"] = pd.to_numeric(df["volume"])

    df = df.set_index("datetime")

    return df, meta_data


allowed_functions = (
    "TIME_SERIES_INTRADAY",
    "TIME_SERIES_INTRADAY_EXTENDED",
    "TIME_SERIES_DAILY",
    "TIME_SERIES_DAILY_ADJUSTED",
)

allowed_intervals = ("1min", "5min")

# Example args
function = "TIME_SERIES_INTRADAY"
symbol = "FCX"
interval = "5min"
outputsize = "compact"


data, metadata = fetch_time_series_daily(
    "FCX",
)
