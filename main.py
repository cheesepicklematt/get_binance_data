from binance.client import Client

from src.utils import getData

# ["BNBBTC","ETHBTC","ADABTC","SOLBTC"]
gd = getData(        
    asset_list=["SOLBTC","ADABTC"],
    time_str = "10 days ago UTC",
    kline_interval = Client.KLINE_INTERVAL_1HOUR
    )

data = gd.return_data(convert_timestamp=True)

data['Close_returns_ADABTC'].min()