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
        asset_list=None,
        time_str = None,
        kline_interval = None
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
    
    def run(
        self,        
        asset_list=None,
        time_str = None,
        kline_interval = None
        ):

        self.asset_list = asset_list
        self.time_str = time_str
        self.kline_interval = kline_interval

        self.extract_data()
        return self.return_data()

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
        self
        ):

        """Iterate over each asset and extract the relevant data
        create returns for open, high, low, close
        """

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



class xHourVolume:
    def __init__(self,hours = None, top_x = None, save_path = None):
        self.client = cred.client

        self.hours = hours
        self.top_x = top_x
        self.save_path = save_path

        info = self.client.get_exchange_info()
        self.symbol_list = [x['symbol'] for x in info['symbols'] if x['symbol'][-3:]=='BTC']

    def run(self):
        gd = getData()
        data = gd.run(
            asset_list=self.symbol_list,
            time_str = str(self.hours)+" hours ago UTC",
            kline_interval = Client.KLINE_INTERVAL_30MINUTE
        )

        vol_list = []
        for symbol in self.symbol_list:
            coin_col = 'Taker buy base asset volume_'+symbol
            btc_col = 'Taker buy quote asset volume_'+symbol
            coin_volume = data[coin_col].sum()
            btc_volume = data[btc_col].sum()
            vol_list.append([symbol,coin_volume,btc_volume])


        excl_list = [
            'WBTCBTC','XMRBTC'
        ]

        vol_df = pd.DataFrame(vol_list,columns=['symbol','coin_volume','btc_volume'])

        symbol_df = vol_df[~vol_df['symbol'].isin(excl_list)].reset_index(drop=True)
        symbol_df = symbol_df.sort_values(by='btc_volume',ascending=False).head(self.top_x)
        
        if self.save_path is not None:
            symbol_df.to_csv(self.save_path,index=False)
        return symbol_df


class tradeData:
    def __init__(self,symbol_list=None,time_str = None):
        self.client = cred.client

        self.time_str = time_str
        self.symbol_list = symbol_list
        self.dict_names = {
            "a": "aggregate tradeId",   
            "p": "price",  
            "q": "quantity",
            "f": "First tradeId",     
            "l": "Last tradeId",        
            "T": "Timestamp", 
            "m": "buyer the maker",        
            "M": "best price match" 
        }

    @staticmethod
    def convert_timestamps_to_ymd_hms(timestamps):
        ymd_hms_formats = []

        for timestamp in timestamps:
            timestamp_seconds = timestamp / 1000
            datetime_obj = dt.datetime.fromtimestamp(timestamp_seconds)
            ymd_hms_format = datetime_obj.strftime('%Y-%m-%d_%H:%M')
            ymd_hms_formats.append(ymd_hms_format)

        return ymd_hms_formats

    def _clean_data(self,symbol):
        self.trade_df['time_formatted'] = self.convert_timestamps_to_ymd_hms(self.trade_df['Timestamp_'+symbol])

    def extract_raw_data(self):
        self.data_dict = {}
        for symbol in self.symbol_list:
            agg_trades = self.client.aggregate_trade_iter(symbol=symbol, start_str=self.time_str)
            agg_trade_list = list(agg_trades)

            trade_data = []
            for tmp_trade in agg_trade_list:
                trade_data.append([tmp_trade[x] for x in self.dict_names])
            
            
            self.trade_df = pd.DataFrame(trade_data,columns=[x+"_"+symbol for x in self.dict_names.values()])
            self._clean_data(symbol)

            self.data_dict[symbol] = self.trade_df

            print(symbol,"trades extracted")