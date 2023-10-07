from binance.client import Client

from src.utils import getData, OrderBookData

'''
gd = getData(        
    asset_list=["BNBBTC","ETHBTC","SOLBTC","XRPBTC","ADABTC","TRXBTC","XMRBTC","AVAXBTC","LTCBTC"],
    time_str = "200 days ago UTC",
    kline_interval = Client.KLINE_INTERVAL_15MINUTE
    )

data = gd.return_data(
    convert_timestamp=True,
    save_csv=True,
    return_data=False)
'''

# orderbook
ob = OrderBookData()
ob.get_current_ob(symbol='BNBBTC')
ob.save_data()

