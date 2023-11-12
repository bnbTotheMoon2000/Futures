# 获取交易symbols 

import requests 
import pandas as pd 
import time 
import datetime as dt 
import pytz 
import urllib.parse 
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec,GridSpecFromSubplotSpec
import warnings 
import numpy as np 
from pyecharts.charts import Line,Pie 
from pyecharts import options as opts
import seaborn as sns 
import math
warnings.filterwarnings('ignore') 

base_asset = 'ENJ'
quote_asset = 'USDT'
# 将 threshold 设置为 0.03 
threshold = 0.003

# 函数
def time_ts(time_str):
    # 输入的时间为utc时间，返回时间戳
    time_obj = dt.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    time_obj = time_obj.replace(tzinfo=pytz.timezone('UTC'))
    return int(time_obj.timestamp())*1000

def ts_time(ts):
    # 输入的时间戳为utc时间戳，返回时间
    time_obj = dt.datetime.utcfromtimestamp(ts/1000)
    time_obj = time_obj.replace(tzinfo=pytz.timezone('UTC'))
    return time_obj.strftime("%Y-%m-%d %H:%M:%S")

# 判断缺失K线数量/ K线无变动数量 
def frozen_kline(row):
    if (row['vol'] ==0) | (row['open'] == row['close'] == row['high']==row['low']):
        return 1 
    else:
        return 0
    
# 定义一个函数来计算平均振幅
def calculate_average_amplitude(df):
    # 计算每一行的振幅
    amplitudes = df['high'] - df['low']
    # 计算振幅的平均值
    average_amplitude = amplitudes.mean()
    return average_amplitude

# 获取交易所spot所有交易对
# 1. okx 
okx_url = "https://www.okx.com/api/v5/public/instruments?instType=SPOT"
okx_symbols = requests.get(okx_url).json()['data']
okx_symbols = list(set([x['instId'] for x in okx_symbols if x['state']=='live']))

# 2. bybit 
bybit_url = "https://api.bybit.com/v5/market/instruments-info?category=spot"
bybit_symbols = requests.get(bybit_url).json()['result']['list']
bybit_symbols = [x['symbol'] for x in bybit_symbols if x['status']=='Trading']

# 3. huobi 
huobi_url = "https://api.huobi.pro/v2/settings/common/symbols"
huobi_symbols = requests.get(huobi_url).json()['data']
huobi_symbols = [x['sc'] for x in huobi_symbols if x['state']=='online']

# 4. hitbtc 
url ='https://api.hitbtc.com/api/3/public/symbol'
hitbtc_resp = requests.get(url).json()
hitbtc_resp = [{x:y} for x,y in hitbtc_resp.items()]
hitbtc_symbols = []
for x in hitbtc_resp:
    for k,v in x.items():
        if v['status']=='working':
            hitbtc_symbols.append(k)

# 5. gate.io
url = "https://api.gateio.ws/api/v4/spot/currency_pairs"
gateio_symbols = requests.get(url).json()
gateio_symbols = [x['id'] for x in gateio_symbols if x['trade_status'] == 'tradable']

# 6. bitmax 
url = "https://ascendex.com/api/pro/v1/cash/products"
bitmax_symbols = requests.get(url).json()['data']
bitmax_symbols = [x['symbol'] for x in bitmax_symbols if x['statusCode']=='Normal']

# 7. kucoin 
url = "https://api.kucoin.com/api/v2/symbols"
kucoin_symbols = requests.get(url).json()['data']
kucoin_symbols = [x['symbol'] for x in kucoin_symbols if x['enableTrading']==True]
# "XRP-USDT" in kucoin_symbols

# 8. MEXc
url = 'https://api.mexc.com/api/v3/exchangeInfo'
mexc_symbols = requests.get(url).json()['symbols']
mexc_symbols = [x['symbol'] for x in mexc_symbols if x['status']=='ENABLED']

# 9. coinbase 
url = 'https://api.exchange.coinbase.com/products'
coinbase_symbols = [x['id'] for x in requests.get(url).json() if x['trading_disabled'] == False]

# 10. huobi 
url = "https://api.huobi.pro/v2/settings/common/symbols"
huobi_symbols = requests.get(url).json()['data']
huobi_symbols = [x['sc'] for x in huobi_symbols if x['state']=='online']

# 11 binance
url = "https://api.binance.com/api/v3/exchangeInfo"
binance_symbols = requests.get(url).json()['symbols']
binance_symbols = [x['symbol'] for x in binance_symbols if x['status']=='TRADING']
binance_symbols[:5]

# 12 bitget 
url = "https://api.bitget.com/api/v2/spot/public/symbols"
bitget_symbols = requests.get(url).json()['data']
bitget_symbols = [x['symbol'] for x in bitget_symbols if x['status']=='online']

# 13. kraken
url = "https://api.kraken.com/0/public/AssetPairs"
kraken_symbols = requests.get(url).json()['result']
kraken_symbols = [k for k,v in kraken_symbols.items()]

# 获取 Kline 接口 


# 查询近8小时的K线数据
backtrack_hours = 8
start = int(time.time()*1000)-1000*60*60*backtrack_hours
end = int(time.time()*1000) - 1000*60*2
print("开始: ",ts_time(start))
print("结束: ",ts_time(end))

# binance 获取index price 示例
symbol = base_asset+quote_asset
url = "https://fapi.binance.com/fapi/v1/indexPriceKlines?pair={symbol}&interval=1m&startTime={start}&endTime={end}".format(symbol=symbol,start=start,end=end)
if requests.get(url).status_code == 200:    
    binance_index = pd.DataFrame(requests.get(url).json()).iloc[:,:5]
    binance_index.columns = ['openTime','open','high','low','close']
    binance_index['openTime'] = pd.to_datetime(binance_index['openTime'],unit='ms')
    binance_index[['open','high','low','close']] = binance_index[['open','high','low','close']].astype(float)
else:
    print("errors from binance",requests.get(url).text)

# binance 获取K线示例
binance_symbol = base_asset+quote_asset
if binance_symbol in binance_symbols:
    binance_url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&startTime={start}&endTime={end}&limit=1000"
    try:
        binance_p = pd.DataFrame(requests.get(binance_url).json()).iloc[:,:6]
        binance_p.columns = ['openTime','open','high','low','close','vol']
        binance_p['openTime'] = pd.to_datetime(binance_p['openTime'],unit='ms')
        binance_p[['open','high','low','close','vol']] = binance_p[['open','high','low','close','vol']].astype(float)
        binance_p.sort_values(by='openTime',ascending=True,inplace=True)
    except Exception as e:
        print(e)
        print("errors from binance",requests.get(url).text)
        print(url)
else:
    binance_p = []
    print(f"symbol: {symbol} not in binance_symbols")

# okx 获取K线示例
symbol = base_asset+'-'+quote_asset
time_step =1000*60*100 
okx_p = pd.DataFrame()

ok_start = start
if symbol in okx_symbols:
    # https://www.okx.com/docs-v5/zh/#order-book-trading-market-data-get-candlesticks-history
    while True:
        url = f"https://www.okx.com/api/v5/market/history-candles?instId={symbol}&bar=1m&after={(ok_start+time_step)}&before={ok_start}"
        try:
            temp_list = list(reversed(requests.get(url).json()['data']))
            temp_okx_p = pd.DataFrame(temp_list).iloc[:,:6]
            temp_okx_p.columns = ['openTime','open','high','low','close','vol']
            temp_okx_p['openTime'] = pd.to_datetime(temp_okx_p['openTime'].astype(int),unit='ms')
            # temp_okx_p['openTime'] = temp_okx_p['openTime'].apply(lambda x: dt.datetime.utcfromtimestamp(int(x)/1000).strftime("%Y-%m-%d %H:%M:%S"))
            temp_okx_p[['open','high','low','close','vol']] = temp_okx_p[['open','high','low','close','vol']].astype(float)
            
            okx_p = pd.concat([okx_p,temp_okx_p],axis=0,ignore_index=True)
            ok_start = int(temp_list[-1][0])
            if ok_start >= end:
                break       
            
        except Exception as e:
            print(e)
            print("errors from OKX",requests.get(url).text)
            print(url)
        okx_p.sort_values(by='openTime',ascending=True,inplace=True)
else:
    temp_okx_p =[]  
    print(f"symbol: {symbol} not in okx_symbols")

# coinbase 获取K线示例
coinbase_time_step = 60*60*5
num_interval = math.ceil((end/1000-start/1000)/coinbase_time_step)
coinbase_start =  int(start/1000)
coinbase_end = coinbase_start+ coinbase_time_step
coinbase_p = pd.DataFrame()
coinbase_symbol = base_asset+'-'+"USD"
if coinbase_symbol in coinbase_symbols:
    for i in range(num_interval):
        url = f"https://api.exchange.coinbase.com/products/{coinbase_symbol}/candles?granularity=60&start={coinbase_start}&end={coinbase_end}"
        resp = requests.get(url).json()
        temp_coinbase = pd.DataFrame(resp)
        temp_coinbase.columns = ['openTime','low','high','open','close','vol']
        temp_coinbase['openTime'] = pd.to_datetime(temp_coinbase['openTime'],unit='s')
        temp_coinbase[['low','high','open','close','vol']] = temp_coinbase[['low','high','open','close','vol']].astype(float)
        coinbase_p = pd.concat([coinbase_p,temp_coinbase],axis=0,ignore_index=True)
        coinbase_start = coinbase_end
        coinbase_end += coinbase_time_step
    
    # coinbase_p = coinbase_p[coinbase_p['openTime'] >= pd.to_datetime(start,unit='ms')]
    coinbase_p.sort_values(by='openTime',ascending=True,inplace=True,ignore_index=True)
    coinbase_p.drop_duplicates(inplace=True,ignore_index=True)
else: 
    coinbase_p = []
    print(f"symbol: {symbol} not in coinbase_symbols")


# hitbtc 获取K线示例 
symbol = base_asset+quote_asset
if symbol in hitbtc_symbols:
    url = "https://api.hitbtc.com/api/3/public/candles/{symbol}?period=M1&from={start}&till={end}&limit=1000&sort=ASC".format(symbol=symbol,start=start,end=end)
    try:
        hitbtc_p = pd.DataFrame(requests.get(url).json())
        hitbtc_p.rename(columns = {"volume":'vol','timestamp':'openTime'},inplace=True)
        hitbtc_p['openTime'] = pd.to_datetime(hitbtc_p['openTime'].str.replace("T"," ").str.split(".",expand=True)[0])
        
        hitbtc_p[['open','close','min','max','vol','volume_quote']] = hitbtc_p[['open','close','min','max','vol','volume_quote']].astype(float)
        hitbtc_p.rename(columns = {'min':'low','max':'high'},inplace=True)
        hitbtc_p.sort_values(by='openTime',ascending=True,inplace=True)
    except Exception as e:
        print("errors from hitbtc",requests.get(url).text)
else:
    hitbtc_p = []
    print(f"symbol: {symbol} not in hitbtc_symbols")

# gateio 获取K线示例
#  "https://api.gateio.ws/api/v4/spot/currency_pairs"
symbol = base_asset+'_'+quote_asset
if symbol in gateio_symbols:
    url = 'https://api.gateio.ws/api/v4/spot/candlesticks'
    params = {
        'currency_pair': symbol,
        'interval': '1m',
        'from': int(start/1000),
        'to': int(end/1000)
    }
    # requests.get(url,params=params)
    url = url + '?'+urllib.parse.urlencode(params)

    try:
        gate_p = pd.DataFrame(requests.get(url).json()).iloc[:,[0,2,3,4,5,6]]
        # gate_p.insert(0,'symbol','BTC_USDT')
        gate_p.columns = ['openTime','close','high','low','open','vol']
        gate_p['openTime'] = pd.to_datetime(gate_p['openTime'].astype(int),unit='s')
        gate_p[['close','high','low','open','vol']] = gate_p[['close','high','low','open','vol']].astype(float)
        gate_p.sort_values(by="openTime",ascending=True,inplace=True)
    except Exception as e:
        print(e)
        print(f"errors from gateio",requests.get(url).text)
else: 
    gate_p = []
    print(f"symbol: {symbol} not in gateio_symbols")

# bitmax 获取K线示例
symbol = base_asset+'/'+quote_asset
if symbol in bitmax_symbols:
    url = 'https://ascendex.com/api/pro/v1/barhist'
    params = {
        'symbol': symbol,
        'interval': '1',
        'from': start,
        'to': end
    }
    params = urllib.parse.urlencode(params)
    url = url + '?'+params

    try:
        result = requests.get(url).json()['data']
        result = [x['data'] for x in result]
        bitmax_p = pd.DataFrame(result)
        bitmax_p = bitmax_p.iloc[:,1:7]
        bitmax_p.columns = ['openTime','open','close','high','low','vol']
        bitmax_p['openTime'] = pd.to_datetime(bitmax_p['openTime'],unit='ms')
        bitmax_p[['open','close','high','low','vol']] = bitmax_p[['open','close','high','low','vol']].astype(float)
        bitmax_p.sort_values(by='openTime',ascending=True,inplace=True)
    except Exception as e:
        print(e)
        print(f"errors from bitmax",requests.get(url).text)
else:
    bitmax_p = []
    print(f"symbol: {symbol} not in bitmax_symbols")

# bybit 获取K线示例
symbol = base_asset+quote_asset
if symbol in bybit_symbols:
    url = f"https://api.bybit.com/v5/market/kline?category=spot&symbol={symbol}&interval=1&start={start}&end={end}&limit=1000"
    try: 
        bybit_p = pd.DataFrame(requests.get(url).json()['result']['list']).iloc[:,:6]
        bybit_p.columns = ['openTime','open','high','low','close','vol']
        bybit_p['openTime'] = pd.to_datetime(bybit_p['openTime'].astype(int),unit='ms')
        bybit_p[['open','high','low','close','vol']] = bybit_p[['open','high','low','close','vol']].astype(float)
        bybit_p.sort_values(by='openTime',ascending=True,inplace=True)
    except Exception as e:
        print(e)
        print(f"errors from bybit",requests.get(url).text)
else:
    bybit_p = []
    print(f"symbol: {symbol} not in bybit_symbols")

# kucoin 获取K线示例
symbol = base_asset+'-'+quote_asset
if symbol in kucoin_symbols:
    url = "https://api.kucoin.com/api/v1/market/candles?type=1min&symbol={symbol}&startAt={start}&endAt={end}".format(symbol=symbol,start=int(start/1000),end=int(end/1000))
    try:
        kucoin_p = pd.DataFrame(requests.get(url).json()['data'])
        kucoin_p.columns = ['openTime','open','close','high','low','vol','turnover']
        kucoin_p['openTime'] = pd.to_datetime(kucoin_p['openTime'].astype(int),unit='s')
        kucoin_p[['open','close','high','low','vol','turnover']] = kucoin_p[['open','close','high','low','vol','turnover']].astype(float)
        kucoin_p.sort_values(by='openTime',ascending=True,inplace=True)
    except Exception as e:
        print(e)
        print(f"errors from kucoin",requests.get(url).text)
else:
    kucoin_p = []
    print(f"symbol: {symbol} not in kucoin_symbols")

# mexc 获取K线示例
symbol = base_asset+quote_asset
if symbol in mexc_symbols and symbol != 'GASUSDT':
    url = f"https://api.mexc.com/api/v3/klines?symbol={symbol}&interval=1m&startTime={start}&endTime={end}&limit=1000"
    try:
        mexc_p = pd.DataFrame(requests.get(url).json()).iloc[:,:6]
        mexc_p.columns = ['openTime','open','high','low','close','vol']
        mexc_p['openTime'] = pd.to_datetime(mexc_p['openTime'],unit='ms')
        mexc_p[['open','high','low','close','vol']] = mexc_p[['open','high','low','close','vol']].astype(float)
        mexc_p.sort_values(by='openTime',ascending=True,inplace=True)
    except Exception as e:
        print(e)
        print(f"errors from mexc",requests.get(url).text)  
else:
    mexc_p = []
    print(f"symbol: {symbol} not in mexc_symbols")

# huobi 获取K线示例
huobi_symbol = symbol.lower()
if huobi_symbol in huobi_symbols:
    url = f"https://api.huobi.pro/market/history/kline?period=1min&size=2000&symbol={huobi_symbol}"
    huobi_p = pd.DataFrame(requests.get(url).json()['data']).iloc[:,:6]
    huobi_p['id'] = pd.to_datetime(huobi_p['id'],unit='s')
    huobi_p.columns = ['openTime','open','close','low','high','vol']
    huobi_p = huobi_p[huobi_p['openTime'] >= pd.to_datetime(start,unit='ms')]
    huobi_p.sort_values(by='openTime',ascending=True,inplace=True)

else:
    huobi_p = []
    print(f"symbol: {symbol} not in huobi_symbols")

# bitget 获取K线示例
symbol = base_asset+quote_asset
bitget_p = pd.DataFrame()
if symbol in bitget_symbols:
    try:
        while True:
            bitget_url = f"https://api.bitget.com/api/v2/spot/market/history-candles?symbol={symbol}&granularity=1min&endTime={end}&limit=200"
            resp = requests.get(bitget_url).json()['data']
            temp_bitget_p = pd.DataFrame(resp).iloc[:,:6]
            temp_bitget_p.columns = ['openTime','open','high','low','close','vol']
            # temp_bitget_p['openTime'] = pd.to_datetime(temp_bitget_p['openTime'],unit='ms')
            # temp_bitget_p[['open','high','low','close','vol']] = temp_bitget_p[['open','high','low','close','vol']].astype(float)
            bitget_p = pd.concat([bitget_p,temp_bitget_p],axis=0,ignore_index=True)
            end = int(resp[0][0])
            if end <= start: 
                break
            
        bitget_p['openTime'] = pd.to_datetime(bitget_p['openTime'].astype(int),unit='ms')
        bitget_p[['open','high','low','close','vol']] = bitget_p[['open','high','low','close','vol']].astype(float)
        bitget_p = bitget_p[bitget_p['openTime'] >= pd.to_datetime(start,unit='ms')]
        bitget_p = bitget_p.sort_values(by='openTime',ascending=True)
    except Exception as e:
        print(e)
        print(f"errors from bitget",requests.get(url).text)
        
else:
    bitget_p = []
    print(f"symbol: {symbol} not in bitget_symbols")
    print(f"https://api.bitget.com/api/v2/spot/market/history-candles?symbol={symbol}&granularity=1min&endTime={end}&limit=1000")

# kraken 获取k线示例  
if base_asset =='BTC':
    kraken_base_asset = 'XBT'
else:
    kraken_base_asset = base_asset
if quote_asset == 'BTC':
    kraken_quote_asset = 'XBT'
else:
    kraken_quote_asset = quote_asset
symbol = kraken_base_asset+kraken_quote_asset
kraken_start = start
kraken_p = pd.DataFrame()
if symbol in kraken_symbols:  
    while True:   
        url = f"https://api.kraken.com/0/public/OHLC?pair={symbol}&interval=1&since={int(kraken_start/1000)}"
        try:
            resp = requests.get(url).json()['result'][symbol]
            
            kranken_temp = pd.DataFrame(resp)
            kraken_p = pd.concat([kraken_p,kranken_temp],axis=0,ignore_index=True)
            kraken_start = int(resp[-1][0])*1000
            if kraken_start >= end:
                kraken_p = kraken_p.iloc[:,[0,1,2,3,4,6]]
                kraken_p.columns = ['openTime','open','high','low','close','vol']
                kraken_p['openTime'] = pd.to_datetime(kraken_p['openTime'],unit='s')
                kraken_p[['open','high','low','close','vol']] = kraken_p[['open','high','low','close','vol']].astype(float)
                kraken_p = kraken_p[kraken_p['openTime'] >= pd.to_datetime(start,unit='ms')]
                break 
        
        except Exception as e:
            print(e)
            print(url)
            break 
else:
    print(f"{symbol} not in kraken_symbols")

# 作图与分析

# 分析
legend_labels = []
legend_labels.append('binance_indexPrice')
legend_labels.append('openTime')
variable_mapping = {'binance':binance_p,'okx':okx_p,'huobi':huobi_p,'coinbase':coinbase_p,
                    'hitbtc':hitbtc_p,'gate.io':gate_p,'bitmax':bitmax_p,'bybit':bybit_p,'kucoin':kucoin_p,'mexc':mexc_p,
                    'bitget':bitget_p,'kraken':kraken_p}

volume_data = pd.DataFrame(columns = ['exchange','vol'])

get_frozen_kline = dict()

merged_price = pd.DataFrame()
merged_price['openTime'] = binance_index['openTime']
# 使用merge 将close 价格拼接
merged_price = pd.merge(merged_price,binance_index[['openTime','close']],on='openTime',how='left').rename(columns={'close':'binance_indexPrice'})
if len(binance_p)>0:
    legend_labels.append('binance')
    volume_data.loc[0] = ['binance',binance_p['vol'].sum()]
    binance_p = pd.merge(left=binance_index,right=binance_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x','')).iloc[:,[0,5,6,7,8,9]]
    get_frozen_kline['binance'] = binance_p['close'].isna().sum() + binance_p.apply(frozen_kline,axis=1).sum()
    merged_price  = pd.merge(merged_price,binance_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'binance'})
if len(okx_p)>0:
    legend_labels.append('okx')
    volume_data.loc[1] = ['okx',okx_p['vol'].sum()]
    okx_p = pd.merge(left=binance_index,right=okx_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x','')).iloc[:,[0,5,6,7,8,9]]
    get_frozen_kline['okx'] = okx_p['close'].isna().sum() + okx_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,okx_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'okx'})
if len(huobi_p)>0:
    legend_labels.append('huobi')
    volume_data.loc[2] = ['huobi',huobi_p['vol'].sum()]
    huobi_p = pd.merge(left=binance_index,right=huobi_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x','')).iloc[:,[0,5,6,7,8,9]]
    get_frozen_kline['huobi'] = huobi_p['close'].isna().sum() + huobi_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,huobi_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'huobi'})
if len(coinbase_p)>0:
    legend_labels.append('coinbase')
    volume_data.loc[3] = ['coinbase',coinbase_p['vol'].sum()]
    coinbase_p = pd.merge(left=binance_index,right=coinbase_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x','')).iloc[:,[0,5,6,7,8,9]]
    get_frozen_kline['coinbase'] = coinbase_p['close'].isna().sum() + coinbase_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,coinbase_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'coinbase'})
if len(hitbtc_p)>0:
    legend_labels.append('hitbtc')
    volume_data.loc[4] = ['hitbtc',hitbtc_p['vol'].sum()]
    hitbtc_p = pd.merge(left=binance_index,right=hitbtc_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x','')).iloc[:,[0,5,6,7,8,9]]
    get_frozen_kline['hitbtc'] = hitbtc_p['close'].isna().sum() + hitbtc_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,hitbtc_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'hitbtc'})
if len(gate_p)>0:
    legend_labels.append('gate.io')
    volume_data.loc[5] = ['gate.io',gate_p['vol'].sum()]
    gate_p = pd.merge(left=binance_index,right=gate_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x','')).iloc[:,[0,5,6,7,8,9]]
    get_frozen_kline['gate.io'] = gate_p['close'].isna().sum() + gate_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,gate_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'gate.io'})
if len(bitmax_p)>0:
    legend_labels.append('bitmax')
    volume_data.loc[6] = ['bitmax',bitmax_p['vol'].sum()]
    bitmax_p = pd.merge(left=binance_index,right=bitmax_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x','')).iloc[:,[0,5,6,7,8,9]]
    get_frozen_kline['bitmax'] = bitmax_p['close'].isna().sum() + bitmax_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,bitmax_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'bitmax'})
if len(bybit_p)>0:
    legend_labels.append('bybit')
    volume_data.loc[7] = ['bybit',bybit_p['vol'].sum()]
    bybit_p = pd.merge(left=binance_index,right=bybit_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x','')).iloc[:,[0,5,6,7,8,9]]
    get_frozen_kline['bybit'] = bybit_p['close'].isna().sum() + bybit_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,bybit_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'bybit'})
if len(kucoin_p)>0:
    legend_labels.append('kucoin')
    volume_data.loc[8] = ['kucoin',kucoin_p['vol'].sum()]
    kucoin_p = pd.merge(left=binance_index,right=kucoin_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x','')).iloc[:,[0,5,6,7,8,9]]
    get_frozen_kline['kucoin'] = kucoin_p['close'].isna().sum() + kucoin_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,kucoin_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'kucoin'})
if len(mexc_p)>0:
    legend_labels.append('mexc')
    volume_data.loc[9] = ['mexc',mexc_p['vol'].sum()]
    mexc_p = pd.merge(left=binance_index,right=mexc_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x','')).iloc[:,[0,5,6,7,8,9]]
    get_frozen_kline['mexc'] = mexc_p['close'].isna().sum() + mexc_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,mexc_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'mexc'})
if len(bitget_p)>0:
    legend_labels.append('bitget')
    volume_data.loc[10] = ['bitget',bitget_p['vol'].sum()]
    bitget_p = pd.merge(left=binance_index,right=bitget_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x','')).iloc[:,[0,5,6,7,8,9]]
    get_frozen_kline['bitget'] = bitget_p['close'].isna().sum() + bitget_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,bitget_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'bitget'})
if len(kraken_p)>0:
    legend_labels.append('kraken')
    volume_data.loc[11] = ['kraken',kraken_p['vol'].sum()]
    kraken_p = pd.merge(left=binance_index,right=kraken_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x','')).iloc[:,[0,5,6,7,8,9]]
    get_frozen_kline['kraken'] = kraken_p['close'].isna().sum() + kraken_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,kraken_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'kraken'})

merged_price = merged_price[legend_labels]
merged_price = merged_price.fillna(method='ffill').fillna(method='bfill')
merged_price.set_index('openTime',inplace=True)

# 如果价格存在异常值，剔除交易所价格
temp_mean = merged_price.apply(lambda x: x.mean(),axis=0)
temp_max_bn = merged_price['binance_indexPrice'].max()
condition = abs(list(temp_mean[temp_mean ==temp_mean.max()].values)[0]) / temp_max_bn >=5 
if condition:
    print('存在价格异常值，需要剔除交易所价格')
    merged_price.drop(columns=list(temp_mean[temp_mean ==temp_mean.max()].index)[0],inplace=True)


# 计算标准差 
diff_df =  merged_price.iloc[:, 1:].subtract(merged_price['binance_indexPrice'], axis=0)
std_devs = diff_df.std()
merged_price.loc['std_dev'] = std_devs

result = merged_price.iloc[-1,1:].to_frame()
result['std_dev'] = result['std_dev'].astype(float) 
result.insert(0,'symbol',symbol)
result = result.reset_index().set_index("symbol").rename(columns = {'index':'exchange'})

exchange_dfs = dict()
for k,v in variable_mapping.items():
    if k in legend_labels:
        exchange_dfs[k] = v 

average_amplitudes = dict()
for key,df in exchange_dfs.items():
    # 计算每个DataFrame的平均振幅并添加到列表中
    avg_amp = calculate_average_amplitude(df)
    average_amplitudes[key] = avg_amp 

amplitude = pd.DataFrame(pd.Series(average_amplitudes))
amplitude.columns = ['amplitude']
result = pd.merge(left = result, right= amplitude, left_on="exchange", right_index = True)
falseKline = pd.Series(get_frozen_kline).to_frame().rename(columns={0:'still_kline'})
result = pd.merge(left=result, right= falseKline, left_on='exchange',right_index=True,suffixes=['','_y'])
result = pd.merge(left=result,right=volume_data,left_on='exchange',right_on='exchange',suffixes=['','_y'])
result = result.sort_values(by=['vol','std_dev','amplitude','still_kline'],ascending=[False,True,True,True])

resample_df = merged_price.iloc[:-1,:]
exchange_names = list(resample_df.columns[1:])
temp_list = []
final_list = []
i = 1
for index,row in resample_df.reset_index().iterrows():
    for i in exchange_names:
        if abs(row[i] - row['binance_indexPrice']) / row['binance_indexPrice'] > threshold:
            temp_list.append(1)
        else:
            temp_list.append(0)

    final_list.append(temp_list)
    temp_list = []
volatile_df = pd.DataFrame(final_list,columns = exchange_names).sum().to_frame().rename(columns={0:'overThreshold'})
result = pd.merge(left= result,right=volatile_df, left_on='exchange',right_index=True,suffixes=['','_y']).sort_values(by='vol',ascending=False)
result.insert(0,'symbol',symbol)

def vol_ratio(row):
    return round(row['vol']/result['vol'].sum()*100,2)
result['volPercent'] = result.apply(vol_ratio,axis=1)
fig = plt.figure(figsize=(20, 10))
gs_main = GridSpec(2, 2, figure=fig)  # 修改为2行1列

# 第一行第一列:线图 
ax1 = fig.add_subplot(gs_main[0,:])
ax1.plot(merged_price.iloc[:-1,1:])
ax1.set_title(f"{symbol} Price Comparison")
ax1.legend(merged_price.columns[1:],loc='upper left')
ax1.grid()
ax1.tick_params(axis='x', rotation=45)

ax2 = fig.add_subplot(gs_main[1,0])
ax2.pie(x=volume_data['vol'],labels=volume_data['exchange'],autopct='%.2f%%',explode=[0.1]*len(volume_data),shadow=True)
ax2.set_title(f'{base_asset+quote_asset} volume propotion')

gs_sub = GridSpecFromSubplotSpec(2, 2, subplot_spec=gs_main[1, 1])
# Colors for each plot
colors = ['blue', 'green', 'red', 'orange']

sub_ax1 = fig.add_subplot(gs_sub[0, 0])
sub_ax1.barh(result['exchange'],result['std_dev'],color=colors[0])
sub_ax1.set_title('std_dev')

sub_ax2 = fig.add_subplot(gs_sub[0, 1])
sub_ax2.barh(result['exchange'],result['vol'],color=colors[1])
sub_ax2.set_title('vol')

sub_ax3 = fig.add_subplot(gs_sub[1, 0])
sub_ax3.barh(result['exchange'],result['overThreshold'],color=colors[2])
sub_ax3.set_title('overThreshold')

sub_ax4 = fig.add_subplot(gs_sub[1, 1])
sub_ax4.barh(result['exchange'],result['still_kline'],color=colors[3])
sub_ax4.set_title('still_kline')

plt.tight_layout()
plt.show()
result



