
from binance.client import Client

from get_binance_data.src.get_data_utils import getData, OrderBookData


gd = getData(        
    asset_list=["BNBBTC"],
    time_str = "1 days ago UTC",
    kline_interval = Client.KLINE_INTERVAL_1MINUTE
    )
data = gd.run()

coin_col = 'Taker buy base asset volume_BNBBTC'
btc_col = 'Taker buy quote asset volume_BNBBTC'
coin_volume = data[coin_col].sum()
btc_volume = data[btc_col].sum()





