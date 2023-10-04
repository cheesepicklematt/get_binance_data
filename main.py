from binance.client import Client

from src.utils import getData


gd = getData(        
    asset_list=["BNBBTC","ETHBTC","ADABTC","SOLBTC"],
    time_str = "100 days ago UTC",
    kline_interval = Client.KLINE_INTERVAL_15MINUTE
    )

data = gd.return_data(
    convert_timestamp=True,
    save_csv=True,
    return_data=False)

