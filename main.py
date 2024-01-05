import requests
import configparser
import pathlib
import pandas as pd
import json
import csv
import glob
from ratelimit import limits, sleep_and_retry

config = configparser.ConfigParser()
config.read("config.cfg")
accesskey = config["alphavantage"]["accesskey"]


class InvalidInputError(Exception):
    pass


class ApiCallFrequencyExceeded(Exception):
    pass


@sleep_and_retry
@limits(calls=3, period=60)  # TODO parameterize
def search(search_word):
    host = "https://www.alphavantage.co"
    url = (
        f"{host}/query?function=SYMBOL_SEARCH&keywords={search_word}&apikey={accesskey}"
    )

    r = requests.get(url)

    data = r.json()

    search_results = [
        (res.get("1. symbol"), res.get("2. name")) for res in data["bestMatches"]
    ]

    return search_results


def fetch_time_series_intraday(symbol, interval, num_months, sleep=60):

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

    output_path = pathlib.Path(f"Data/{interval}/{symbol}")

    output_path.mkdir(parents=True, exist_ok=True)

    @sleep_and_retry
    @limits(calls=3, period=60)  # TODO parameterize this
    def do_download(s, url):
        print(f"Doing download: {url}")
        download = s.get(url)
        decoded_content = download.content.decode("utf-8")
        data = list(csv.reader(decoded_content.splitlines(), delimiter=","))

        if (
            "Thank you for using Alpha Vantage! Our standard API call frequency is 5 calls"
            in download.text
        ):
            raise ApiCallFrequencyExceeded(download.text)
        return data

    with requests.Session() as s:
        for month in months:
            url = f"{host}/query?function=TIME_SERIES_INTRADAY&datatype=csv&outputsize=compact&symbol={symbol}&interval={interval}&slice={month}&apikey={accesskey}"

            data = do_download(s, url)

            df = pd.DataFrame(data[1:], columns=data[0])

            df["time"] = pd.to_datetime(df["time"])
            df["open"] = pd.to_numeric(df["open"])
            df["high"] = pd.to_numeric(df["high"])
            df["low"] = pd.to_numeric(df["low"])
            df["close"] = pd.to_numeric(df["close"])
            df["volume"] = pd.to_numeric(df["volume"])

            df = df.set_index("time")
            filename = str(df.index[0]).replace(" ", "_") + ".parquet"
            df.to_parquet(output_path.joinpath(filename).as_posix(), engine="pyarrow")


@sleep_and_retry
@limits(calls=3, period=60)  # TODO parameterize this
def fetch_time_series_daily_adjusted(symbol):
    host = "https://www.alphavantage.co"
    url = f"{host}/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&outputsize=full&datatype=csv&apikey={accesskey}"
    r = requests.get(url)
    decoded_content = r.content.decode("utf-8")
    data = list(csv.reader(decoded_content.splitlines(), delimiter=","))
    df = pd.DataFrame(data[1:], columns=data[0])

    output_path = pathlib.Path(f"Data/day/{symbol}")

    output_path.mkdir(parents=True, exist_ok=True)

    df["time"] = pd.to_datetime(df["timestamp"])
    df["open"] = pd.to_numeric(df["open"])
    df["high"] = pd.to_numeric(df["high"])
    df["low"] = pd.to_numeric(df["low"])
    df["close"] = pd.to_numeric(df["close"])
    df["volume"] = pd.to_numeric(df["volume"])

    df = df.set_index("time")
    del df["timestamp"]
    filename = str(df.index[0]).replace(" ", "_") + ".parquet"
    df.to_parquet(output_path.joinpath(filename).as_posix(), engine="pyarrow")


def json_to_dataframe(data):
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
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["open"] = pd.to_numeric(df["open"])
    df["high"] = pd.to_numeric(df["high"])
    df["low"] = pd.to_numeric(df["low"])
    df["close"] = pd.to_numeric(df["close"])
    df["volume"] = pd.to_numeric(df["volume"])

    df = df.set_index("timestamp")

    return df, meta_data


def load_data_files(glob_pattern):
    return pd.concat(
        [pd.read_parquet(file_, engine="pyarrow") for file_ in glob.glob(glob_pattern)]
    )


if __name__ == "__main__":

    # Example args
    symbol = "FCX"
    interval = "5min"
    months = 12

    # Fetch some data from api
    fetch_time_series_intraday(symbol, interval, months)

    # Now read all the data back into a big dataframe

    df = load_data_files("Data/1min/FCX/*.parquet")
