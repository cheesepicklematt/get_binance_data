import pandas as pd


from binance.client import Client

from get_binance_data.config import cred
from get_binance_data.src.get_data_utils import xHourVolume


vd = xHourVolume(hours=12,top_pct=0.5,save_path='')
volume_data = vd.run()


