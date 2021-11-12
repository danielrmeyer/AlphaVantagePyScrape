import requests
import configparser
import pandas as pd
import json
import csv

from requests.api import head

config = configparser.ConfigParser()
config.read("config.cfg")
accesskey = config["alphavantage"]["accesskey"]


def query_api(function, symbol, interval, outputsize, accesskey):
    url = f"https://www.alphavantage.co/query?function={function}&symbol={symbol}&interval={interval}&apikey=accesskey"

    r = requests.get(url)
    data = r.json()
    return data


class InvalidInputError(Exception):
    pass


def fetch_time_series_intraday(
    symbol,
    interval,
    num_months,
    adjusted="true",
    outputsize="compact",
    datatype="json",
):

    allowed_intervals = ("1min", "5min", "15min", "30min", "60min")

    host = "https://www.alphavantage.co"

    if interval not in allowed_intervals:
        raise InvalidInputError(f"{interval} not an allowed value for interval.")

    if num_months < 1 or num_months > 24:
        raise InvalidInputError(
            f"{str(num_months)} is out of range.  num_months must be between 1 and 24"
        )

    allowed_slices = [f"year{y}month{m}" for y in [1, 2] for m in range(1, 13)]

    months = allowed_slices[slice(0, int(num_months))]

    df_chunks = []

    with requests.Session() as s:
        for month in months:
            url = f"{host}/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={symbol}&interval={interval}&slice={month}&apikey={accesskey}"

            download = s.get(url)
            decoded_content = download.content.decode("utf-8")
            data = list(csv.reader(decoded_content.splitlines(), delimiter=","))
            df = pd.DataFrame(data[1:], columns=data[0])

            df["time"] = pd.to_datetime(df["time"])
            df["open"] = pd.to_numeric(df["open"])
            df["high"] = pd.to_numeric(df["high"])
            df["low"] = pd.to_numeric(df["low"])
            df["close"] = pd.to_numeric(df["close"])
            df["volume"] = pd.to_numeric(df["volume"])

            df = df.set_index("time")

            df_chunks.append(df)

    return df_chunks


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


# Example args
function = "TIME_SERIES_INTRADAY"
symbol = "FCX"
interval = "5min"
outputsize = "compact"


data, metadata = fetch_time_series_daily(
    "FCX",
)
