import pandas as pd
import os
from binance.client import Client
import datetime as dt
import time
import queue
import threading
import random


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
        kline_interval = None,
        threading = True,
        num_threads = 4,
        lag_sec = 0.2
        ):
        """asset list is a list of assets
        time_str and kline_interval are a single value
        """
        self.asset_list = asset_list
        self.time_str = time_str
        self.kline_interval = kline_interval

        # static variables
        self.threading = threading
        self.num_threads = num_threads
        self.lag_sec = lag_sec


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

    @staticmethod
    def convert_timestamps_to_ymd_hms(timestamps):
        ymd_hms_formats = []

        for timestamp in timestamps:
            timestamp_seconds = timestamp / 1000
            datetime_obj = dt.datetime.fromtimestamp(timestamp_seconds)
            ymd_hms_format = datetime_obj.strftime('%Y-%m-%d_%H:%M:%S')
            ymd_hms_formats.append(ymd_hms_format)

        return ymd_hms_formats

    def get_klines(self,symbol_list):
        """Iterate over each asset and extract the relevant data
        create returns for open, high, low, close
        """

        all_data_raw = []
        for asset in symbol_list:
            try:
                data = self.client.get_historical_klines(asset, self.kline_interval, self.time_str)
                data = pd.DataFrame(data,columns=self.kline_dict.keys())
                del data['Can be ignored'],data['Close time']
                data = data.apply(pd.to_numeric)

                data.columns = [x+'_'+asset if x.find('time')==-1 else x for x in data.columns]

                all_data_raw.append(data)
                print(asset,f"data extracted    Lag sec: {self.lag_sec}")
                time.sleep(self.lag_sec)

            except Exception as e:
                print(e)
                print(asset,"data NOT extracted - EXCEPTION OCCURED")
        
        return all_data_raw

    def merge_data(self,prices_list=None):
        num_assets = len(prices_list)

        full_open_time = [x['Open time'] for x in prices_list]
        full_open_time = list(set([y for x in full_open_time for y in x]))
        
        final_data = pd.DataFrame(full_open_time,columns=['Open time'])
        final_data = final_data.sort_values(by="Open time")
        for i in range(num_assets):
            final_data = pd.merge(final_data, prices_list[i], on='Open time', how='left')
        return final_data

    def return_data(self,data_queue=None,prices_list=None,format_time = False):
        final_data = self.merge_data(prices_list=prices_list)
        if format_time:
            final_data['Open time formatted'] = self.convert_timestamps_to_ymd_hms(final_data['Open time']) 

        if self.threading:
            data_queue.put(final_data)

    def get_data_thread(self,symbol_list,data_queue=None):
        prices_data = self.get_klines(symbol_list)
        self.return_data(data_queue = data_queue, prices_list = prices_data)

    def split_list(self,lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def extract_data(
        self
        ):
        # bot to be threaded
        random.shuffle(self.asset_list)
        symbol_splits = list(self.split_list(self.asset_list,self.num_threads))

        result_queue = queue.Queue()
        threads = []
        for symbol_list in  symbol_splits:
            thread = threading.Thread(target=self.get_data_thread, kwargs={"symbol_list":symbol_list,"data_queue":result_queue})
            threads.append(thread)
            thread.start()
            time.sleep(0.2)

        # Wait for all threads to finish
        for thread in threads:
            thread.join()
        
        self.data_list = []
        while not result_queue.empty():
            result = result_queue.get()
            if not result.empty:
                self.data_list.append(result)
        
        export_data = self.merge_data(prices_list=self.data_list)
        export_data['Open time formatted'] = self.convert_timestamps_to_ymd_hms(export_data['Open time']) 

        return export_data


    


class xHourVolume:
    def __init__(self,hours = None, top_x = None, save_path = None):
        self.client = cred.client

        self.hours = hours
        self.top_x = top_x
        self.save_path = save_path

        info = self.client.get_exchange_info()
        self.symbol_list = [x['symbol'] for x in info['symbols'] if x['symbol'][-3:]=='BTC']

    def run(self):
        gd = getData(
            asset_list=self.symbol_list,
            time_str = str(self.hours)+" hours ago UTC",
            kline_interval = Client.KLINE_INTERVAL_30MINUTE,
            num_threads = 8
        )
        data = gd.extract_data()

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


def get_price_qty_round_num(symbol_list):
    client = cred.client
    round_num_details = []
    for symbol in symbol_list:
        symbol_info = client.get_symbol_info(symbol)['filters']

        price_round_num = symbol_info[0]['minPrice'].rstrip('0')
        price_round_num = len(price_round_num.split('.')[1]) if price_round_num.split('.')[1]!='0' else 0

        min_trade = symbol_info[1]['minQty'].rstrip('0')
        qty_round_num = len(min_trade.split('.')[1]) if min_trade.split('.')[1]!='0' else 0

        round_num_details.append([symbol,price_round_num,qty_round_num])
        time.sleep(0.05)

    return pd.DataFrame(round_num_details,columns=["symbol","price_round_num","qty_round_num"])