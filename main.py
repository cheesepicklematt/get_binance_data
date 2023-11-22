import pandas as pd
import datetime as dt

from binance.client import Client

from get_binance_data.config import cred
from get_binance_data.src.get_data_utils import xHourVolume, getData



client = cred.client
info = client.get_exchange_info()
symbol_list_full = [x['symbol'] for x in info['symbols'] if x['symbol'][-3:]=='BTC']

gd = getData(
    asset_list = symbol_list_full,
    time_str = "5 day ago UTC",
    kline_interval = '1m',
    threading = True,
    num_threads=10
)


data = gd.extract_data()
