import pandas as pd
import datetime as dt

from binance.client import Client

from get_binance_data.config import cred
from get_binance_data.src.get_data_utils import xHourVolume, getData



client = cred.client
info = client.get_exchange_info()
symbol_list_full = [x['symbol'] for x in info['symbols'] if x['symbol'][-3:]=='BTC'][0:50]


gd = getData(
    asset_list = symbol_list_full,
    time_str = "1 day ago UTC",
    kline_interval = '1h',
    threading = True
)


data = gd.extract_data()


gd.data_list