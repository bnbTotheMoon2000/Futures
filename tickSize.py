import requests 
import pandas 

kline_url = "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1d&startTime=1706745600000&endTime=1707350399000"
exchange_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
exchange_resp = requests.get(exchange_url).json()['symbols']
symbols = [x['symbol'] for x in exchange_resp if x['status']=='TRADING' and x['quoteAsset']!='BTC']
containers = []
for symbol in symbols:
    symbol_dict = dict()
    kline_url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1d&startTime=1706745600000&endTime=1707350399000"
    resp = requests.get(kline_url).json()
    symbol_dict['symbol'] = symbol
    symbol_dict['data'] = round(np.array([(float(x[7])/1000000) for x in resp]).mean(),2)
    containers.append(symbol_dict)
