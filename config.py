import os
import json
from binance.client import Client


class getCred:
    def __init__(self):
        self.pathCred = os.path.join(os.path.expanduser('~'),'0_cred')
        binanceApiDict = self.readDictFromTxt(os.path.join(self.pathCred,'binanceAPI_2.txt'))
        binanceAPI = binanceApiDict['binanceAPI']
        binanceAPISecret = binanceApiDict['binanceAPISecret']
        self.client = Client(binanceAPI, binanceAPISecret)
        self.cmcAPI = open(os.path.join(self.pathCred,'cmcAPI.txt')).read()

    def readDictFromTxt(self,path):
        # reading the data from the file
        with open(path) as f:
            data = f.read()
        return json.loads(data)

cred = getCred()

binance_dir = os.path.join('Documents','1_coding','1_trading','binance_stuff')
if os.name == 'posix':
    save_dir = os.path.join(os.path.expanduser('~'),binance_dir,'execute_trades','data','testing_data')
else:
    save_dir = os.path.join(os.path.expanduser('~'),'python_dir','6_crypto','3_back_test','backtest_stat_arb','data')

