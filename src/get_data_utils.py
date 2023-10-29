import pandas as pd
import os
from binance.client import Client
import datetime as dt

from get_binance_data.config import cred, save_dir


class OrderBookData:
    def __init__(self):
        self.client = cred.client

    def get_current_ob(self,symbol=None):
        order_book = self.client.get_order_book(symbol=symbol)
        self.bids_df = pd.DataFrame(order_book['bids'],columns = ['price','qty'])
        self.asks_df = pd.DataFrame(order_book['asks'],columns = ['price','qty'])

    def save_data(self):
        self.bids_df.to_csv(os.path.join(save_dir,'bids.csv'),index=False)
        self.asks_df.to_csv(os.path.join(save_dir,'asks.csv'),index=False)


class getData:
    def __init__(
        self,
        asset_list=["BNBBTC"],
        time_str = "1 day ago UTC",
        kline_interval = Client.KLINE_INTERVAL_15MINUTE
        ):
        """asset list is a list of assets
        time_str and kline_interval are a single value
        """
        self.asset_list = asset_list
        self.time_str = time_str
        self.kline_interval = kline_interval

        # static variables
        self.client = cred.client
        self.kline_dict = {'Open time':0,
              'Open':1,
              'High':2,
              'Low':3,
              'Close':4,
              'Volume':5,
              'Close time':6,
              'Quote asset volume':7,
              'Number of trades':8,
              'Taker buy base asset volume':9,
              'Taker buy quote asset volume':10,
              'Can be ignored':11}
    
    def run(self):
        self.extract_data()
        self.return_data()

    @staticmethod
    def convert_timestamps_to_ymd_hms(timestamps):
        ymd_hms_formats = []

        for timestamp in timestamps:
            timestamp_seconds = timestamp / 1000
            datetime_obj = dt.datetime.fromtimestamp(timestamp_seconds)
            ymd_hms_format = datetime_obj.strftime('%Y-%m-%d_%H:%M:%S')
            ymd_hms_formats.append(ymd_hms_format)

        return ymd_hms_formats

    
    def extract_data(
        self,   
        asset_list=["BNBBTC"],
        time_str = "1 day ago UTC",
        kline_interval = Client.KLINE_INTERVAL_15MINUTE
        ):

        """Iterate over each asset and extract the relevant data
        create returns for open, high, low, close
        """
        self.asset_list = asset_list
        self.time_str = time_str
        self.kline_interval = kline_interval

        self.all_data_raw = []
        for asset in self.asset_list:
            data = self.client.get_historical_klines(asset, self.kline_interval, self.time_str)
            data = pd.DataFrame(data,columns=self.kline_dict.keys())
            del data['Can be ignored'],data['Close time']
            data = data.apply(pd.to_numeric)

            data.columns = [x+'_'+asset if x.find('time')==-1 else x for x in data.columns]

            self.all_data_raw.append(data)
            print(asset,"data extracted")

    def merge_data(self):
        num_assets = len(self.all_data_raw)

        if num_assets == 1:
            self.final_data = self.all_data_raw[0]
        elif num_assets == 2:
            self.final_data = pd.merge(self.all_data_raw[0], self.all_data_raw[1], on='Open time', how='left')
        elif num_assets > 2:
            self.final_data = pd.merge(self.all_data_raw[0], self.all_data_raw[1], on='Open time', how='left')
            for i in range(2,num_assets):
                self.final_data = pd.merge(self.final_data, self.all_data_raw[i], on='Open time', how='left')

    def return_data(self,convert_timestamp=True,save_csv=False,return_data=True):
        self.merge_data()

        if convert_timestamp:
            self.final_data['Open time formatted'] = self.convert_timestamps_to_ymd_hms(self.final_data['Open time']) 

        if save_csv:
            self.final_data.to_csv(os.path.join(save_dir,'raw_data.csv'),index=False)

        if return_data:
            return self.final_data