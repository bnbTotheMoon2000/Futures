import aiohttp
import asyncio
import pandas as pd 
import time 
import datetime as dt 
import time 
import pytz 
import requests 
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec,GridSpecFromSubplotSpec
import numpy as np 
import warnings 
warnings.filterwarnings('ignore') 

def time_ts(time_str):
    if isinstance(time_str,int):
        return int(time.time())*1000 - 1000 * 60 * 60 * int(time_str)
    else:
        try:
            time_obj = dt.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            time_obj = time_obj.replace(tzinfo=pytz.timezone('UTC'))
            return int(time_obj.timestamp())*1000
        except:
            return int(time.time())*1000

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


async def fetch(session, url,headers=None):
    async with session.get(url,headers=headers) as response:
        response.raise_for_status()
        return await response.json()
    
# exchange Info 接口
async def get_exchange_info():
    urls = {
        'okx_products':"https://www.okx.com/api/v5/public/instruments?instType=SPOT",
        'bybit_products':"https://api.bybit.com/v5/market/instruments-info?category=spot",
        "huobi_products"  : "https://api.huobi.pro/v2/settings/common/symbols",
        'hitbtc_products': 'https://api.hitbtc.com/api/3/public/symbol',
        'gateio_products':"https://api.gateio.ws/api/v4/spot/currency_pairs",
        "bitmax_products": "https://ascendex.com/api/pro/v1/cash/products",
        "kucoin_products":"https://api.kucoin.com/api/v2/symbols",
        'mexc_products':'https://api.mexc.com/api/v3/exchangeInfo',
        'coinbase_products':'https://api.exchange.coinbase.com/products',
        'binance_products':'https://api.binance.com/api/v3/exchangeInfo',
        'bitget_products': "https://api.bitget.com/api/v2/spot/public/symbols",
        'kraken_products':"https://api.kraken.com/0/public/AssetPairs"
    }

    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls.values():
            tasks.append(fetch(session, url))
        
        results = await asyncio.gather(*tasks)
        return {name: result for name, result in zip(urls.keys(), results)}
    
# 获取当前事件循环
loop = asyncio.get_event_loop()

# 在当前事件循环中运行main()协程
if loop.is_running():
    # 如果事件循环已经运行，则创建一个任务并在完成后获取结果
    task = loop.create_task(get_exchange_info())
    results = await task  # 这里应该使用await来获取任务结果
else:
    # 如果没有事件循环在运行，使用run_until_complete
    results = loop.run_until_complete(get_exchange_info())

okx_symbols = results['okx_products']['data']
okx_symbols = list(set([x['instId'] for x in okx_symbols if x['state']=='live']))
bybit_symbols = results['bybit_products']['result']['list']
bybit_symbols = list(set([x['symbol'] for x in bybit_symbols if x['status']=='Trading']))
huobi_symbols = results['huobi_products']['data']
huobi_symbols = list(set([x['sc'] for x in huobi_symbols if x['state']=='online']))
hitbtc_symbols = []
hitbtc_resp = [{x:y} for x,y in results['hitbtc_products'].items()]
for x in hitbtc_resp:
    for k,v in x.items():
        if v['status']=='working':
            hitbtc_symbols.append(k)
gateio_symbols = [x['id'] for x in results['gateio_products'] if x['trade_status'] == 'tradable']
bitmax_symbols = [x['symbol'] for x in results['bitmax_products']['data'] if x['statusCode'] == 'Normal']
kucoin_symbols = [x['symbol'] for x in results['kucoin_products']['data'] if x['enableTrading']==True]
mexc_symbols = [x['symbol'] for x in results['mexc_products']['symbols'] if x['status']=='ENABLED']
coinbase_symbols = [x['id'] for x in results['coinbase_products'] if x['trading_disabled'] == False]
binance_symbols = [x['symbol'] for x in results['binance_products']['symbols'] if x['status']=='TRADING']
bitget_symbols = [x['symbol'] for x in results['bitget_products']['data'] if x['status']=='online']
kraken_symbols = [k for k,v in results['kraken_products']['result'].items()] 

start_input = input("Please enter the start time (yyyy-mm-dd HH:MM:SS or number of hours ago): ")
end_input = input("Please enter the end time (yyyy-mm-dd HH:MM:SS or 'now'): ")

if end_input.upper() == 'NOW':
    end = int(time.time())*1000
else:
    end = time_ts(end_input)
if isinstance(start_input, str) and start_input.isdigit():
    start = time_ts(int(start_input))
else:
    start = time_ts(start_input)

base_asset = input("Please enter the base asset: ").upper()
quote_asset = input("Please enter the quote asset: ").upper()

print(f"start: {start}, end: {end}")

def sorting(df):
    df = df.iloc[:,:6]
    columns_name = ['openTime','open','high','low','close','vol']
    df.columns = columns_name
    df['openTime'] = df['openTime'].astype(int)
    if len(str(df.iat[0,0])) > 11:
        df['openTime'] = pd.to_datetime(df['openTime'],unit='ms')
    else:
        df['openTime'] = pd.to_datetime(df['openTime'],unit='s')
    df[['open','high','low','close','vol']] = df[['open','high','low','close','vol']].astype(float)
    df.sort_values(by='openTime',inplace=True)
    df.drop_duplicates(inplace=True)
    return df 


def components_check(df,lst):
    if df.iat[0,1] in lst:
        df['effective'] =True
    else:
        df['effective'] =False

async def fetch(session, url,headers=None):
    async with session.get(url,headers=headers) as response:
        response.raise_for_status()
        return await response.json()

async def get_binance_klines(session, base_asset,quote_asset, start, end):
    ''' 
    binance Kline API
    https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
    '''
    time_step = 1000 * 60 * 1000  # 对于binance
    symbol = base_asset + quote_asset
    if symbol in binance_symbols:
        df = pd.DataFrame()
        attempts = 1
        max_attempts = 5
        # try:
        while True:
            url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&startTime={start}&endTime={start+time_step}&limit=1000"
            try:
                data = await fetch(session, url)
                temp = pd.DataFrame(data).iloc[:, :6]
                df = pd.concat([df, temp], axis=0, ignore_index=True)
                start = start+time_step
                attempts = 1
                if start >= end:
                    break
            except Exception as e:
                attempts +=1
                print(f"Error comes from not in binance, {attempts} times request: {e}",url)
            if attempts == max_attempts:
                print(f"Max attempts reached for {symbol}")
                break
        return df
    else:
        df = []
        print(f"{symbol} not in binance")
        return df 

async def get_binanceIndex_klines(session, base_asset,quote_asset, start, end):
    """
    binance index Kline API
    https://binance-docs.github.io/apidocs/futures/en/#index-price-kline-candlestick-data
    """
    symbol = base_asset + quote_asset
    if symbol in binance_symbols:
        df = pd.DataFrame()
        attempts = 1
        max_attempts = 5
        # try:
        try:
            url = f"https://fapi.binance.com/fapi/v1/indexPriceKlines?pair={symbol}&interval=1m&startTime={start}&endTime={end}&limit=1500"
            data = await fetch(session, url)
            temp = pd.DataFrame(data).iloc[:, :6]
            df = pd.concat([df, temp], axis=0, ignore_index=True)
        except Exception as e:
            print(f"Error comes from not in binance, {attempts} times request: {e}",url)
        return df
    else:
        df = []
        print(f"{symbol} not in binanceIndex")
        return df 
    
async def get_okx_klines(session, base_asset,quote_asset, start, end):
    """ 
    OKX Kline API
    https://www.okx.com/docs-v5/en/#order-book-trading-market-data-get-candlesticks-history
    """
    time_step = 1000 * 60 * 100  # 对于OKX
    symbol = base_asset + "-" + quote_asset
    if symbol in okx_symbols:
        df = pd.DataFrame()
        attempts = 1
        max_attempts = 5
        while True:
            url = f"https://www.okx.com/api/v5/market/history-candles?instId={symbol}&bar=1m&after={(start+time_step)}&before={start}"
            try:
                data = await fetch(session, url)
                temp = pd.DataFrame(data['data']).iloc[:,:6]
                df = pd.concat([df,temp],axis=0,ignore_index=True)
                start = start+ time_step
                attempts = 1
                if start >= end:
                    break   
            except Exception as e:
                attempts +=1
                print(f"Error comes from not in okx: {e}",url)
            if attempts == max_attempts:
                print(f"Max attempts reached for {symbol}")
                break
        return df
    else:
        df = []
        print(f"{symbol} not in okx")
        return df 

async def get_bybit_klines(session, base_asset,quote_asset, start, end):
    """ 
    Bybit Kline API
    https://bybit-exchange.github.io/docs/inverse/#t-querykline
    """
    time_step = 1000 * 60 * 1000  # 对于Bybit
    symbol = base_asset + quote_asset
    if symbol in bybit_symbols:
        df = pd.DataFrame()
        attempts = 1
        max_attempts = 5
        while True:
            url = f"https://api.bybit.com/v5/market/kline?category=spot&symbol={symbol}&interval=1&start={start}&end={start+ time_step}&limit=1000"
            try:
                data = await fetch(session, url)
                temp = pd.DataFrame(data['result']['list']).iloc[:,:6]
                df = pd.concat([df,temp],axis=0,ignore_index=True)
                start = start+ time_step
                attempts = 1
                if start >= end:
                    break   
            except Exception as e:
                attempts +=1
                print(f"Error comes from not in bybit: {e}")
            if attempts == max_attempts:
                print(f"Max attempts reached for {symbol}")
                break
        return df
    else:
        df = []
        print(f"{symbol} not in okx")
        return df

async def get_hitbtc_klines(session, base_asset,quote_asset, start, end):
    """
    Hitbtc Kline API
    https://api.hitbtc.com/#spot-trading
       """
    time_step = 1000 * 60 * 1000  # 对于Hitbtc
    symbol = base_asset + quote_asset

    if symbol in hitbtc_symbols:
        df = pd.DataFrame()
        attempts = 1
        max_attempts = 5
        while True:
            url = f"https://api.hitbtc.com/api/3/public/candles/{symbol}?period=M1&from={start}&till={start+ time_step}&limit=1000&sort=ASC"
            try:
                data = await fetch(session, url)
                temp = pd.DataFrame(data)
                df = pd.concat([df,temp],axis=0,ignore_index=True)
                start = start+ time_step
                attempts = 1
                if start >= end:
                    break 
                
            except Exception as e:
                print(f"Error comes from not in okx: {e}")
                return []
            if attempts == max_attempts:
                print(f"Max attempts reached for {symbol}")
                break
        return df
    else:
        df = []
        print(f"{symbol} not in hitbtc")
        return df

async def get_huobi_klines(session, base_asset,quote_asset):
    """ 
    huobi Kline API 
    https://www.htx.com/en-us/opend/newApiPages/?id=7ec4a4da-7773-11ed-9966-0242ac110003
    """
    symbol = base_asset + quote_asset
    symbol = symbol.lower()
    if symbol in huobi_symbols:
        url = f"https://api.huobi.pro/market/history/kline?period=1min&size=2000&symbol={symbol}"
        try:
            data = await fetch(session, url)
            df = pd.DataFrame(data['data']).iloc[:,:6]
            return df
        except Exception as e:
            print(f"Error comes from not in huobi: {e}")
            return []
    else:
        df = []
        print(f"{symbol} not in huobi")
        return df
    
async def get_gateio_klines(session, base_asset,quote_asset,start,end):
    """ 
    gateio Kline API
    https://www.gate.io/docs/developers/apiv4/zh_CN/#%E5%B8%82%E5%9C%BA-k-%E7%BA%BF%E5%9B%BE
    """
    symbol = base_asset+'_'+quote_asset
    gate_start = int(start/1000)
    gate_end = int(end/1000)
    time_step =  60*100  # 对于gateio
    if symbol in gateio_symbols:
        df = pd.DataFrame()
        attempts = 1
        max_attempts = 5
        while True:
            url = f'https://api.gateio.ws/api/v4/spot/candlesticks?currency_pair={symbol}&interval=1m&from={gate_start}'
            try:
                data = await fetch(session, url)
                temp = pd.DataFrame(data)
                df = pd.concat([df,temp],axis=0,ignore_index=True)
                gate_start = gate_start+ time_step
                attempts = 1
                if gate_start >= gate_end:
                    break
            except Exception as e:
                print(f"Error comes from not in gateio: {e}")
                return []
            if attempts == max_attempts:
                print(f"Max attempts reached for {symbol}")
                break
        return df
    else:
        df = []
        print(f"{symbol} not in hitbtc")
        return df

async def get_bitmax_klines(session,base_asset,quote_asset,start,end):
    ''' 
    bitmax Kline API 
    https://ascendex.github.io/ascendex-pro-api/#historical-bar-data
    '''
    symbol = base_asset+'/'+quote_asset
    time_step = 1000 * 60 * 500
    if symbol in bitmax_symbols:
        df = pd.DataFrame()
        attempts = 1
        max_attempts = 5
        while True:
            url = f'https://ascendex.com/api/pro/v1/barhist?symbol={symbol}&interval=1&from={start}&to={start+ time_step}&n=500'
            try:
                data = await fetch(session, url)
                data = data['data']
                data =[x['data'] for x in data]
                temp = pd.DataFrame(data).iloc[:,1:7]
                df = pd.concat([df,temp],axis=0,ignore_index=True)
                start = start+ time_step
                attempts = 1
                if start >= end:
                    break
            except Exception as e:
                print(f"Error comes from not in bitmax: {e}",url)
            
            if attempts == max_attempts:
                print(f"Max attempts reached for {symbol}")
                break
        return df 
    else:
        df = []
        print(f"{symbol} not in bitmax")
        return df

async def get_kucoin_kliens(session,base_asset,quote_asset,start,end):
    """ 
    kucoin Kline API 
    https://www.kucoin.com/docs/rest/spot-trading/market-data/get-klines
    """
    symbol = base_asset+'-'+quote_asset
    if symbol in kucoin_symbols:
        url = "https://api.kucoin.com/api/v1/market/candles?type=1min&symbol={symbol}&startAt={start}&endAt={end}".format(symbol=symbol,start=int(start/1000),end=int(end/1000))
        try:
            data = await fetch(session, url)
            df = pd.DataFrame(data['data'])
            return df
        except Exception as e:
            print(f"Error comes from not in kucoin: {e}")
            return []
    else:
        df = []
        print(f"{symbol} not in kucoin")
        return df

async def get_mexc_klines(session,base_asset,quote_asset,start,end):
    """ 
    mexc Kline API 
    https://mexcdevelop.github.io/apidocs/spot_v3_en/#kline-candlestick-data
    """
    symbol = base_asset+quote_asset
    time_step = 1000 * 60 * 1000
    if symbol in mexc_symbols:
        df = pd.DataFrame()
        attempts = 1
        max_attempts = 5
        
        i = 1
        while True:
            url = f"https://api.mexc.com/api/v3/klines?symbol={symbol}&interval=1m&startTime={start}&endTime={start+time_step}&limit=1000"
            try:
                data = await fetch(session, url)
                temp = pd.DataFrame(data)
                df = pd.concat([df,temp],axis=0,ignore_index=True)
                start = data[-1][0]
                time_step = int(end - start)
                i+=1
                attempts = 1
                if i == 3:
                    break 

            except Exception as e:
                print(f"Error comes from not in mexc: {e}",url)
             
            if attempts == max_attempts:
                print(f"Max attempts reached for {symbol}")
                break
        return df
    else:
        df = []
        print(f"{symbol} not in mexc")
        return df
    
async def get_coinbase_klines(session,base_asset,quote_asset,start,end):
    """ 
    coinbase kline api
    https://docs.cloud.coinbase.com/exchange/reference/exchangerestapi_getproductcandles
    """
    time_step = 60*60*5
    start =  int(start/1000)
    end = int(end/1000)
  
    if quote_asset == 'USDT':
        coinbase_symbol = base_asset+'-'+"USD"
    else:
        coinbase_symbol = base_asset+'-'+quote_asset
    if coinbase_symbol in coinbase_symbols:
        df = pd.DataFrame()
        attempts = 1
        max_attempts = 5
        while True:
            url = f"https://api.exchange.coinbase.com/products/{coinbase_symbol}/candles?granularity=60&start={start}&end={start+time_step}"
            try:
                data = await fetch(session, url)
                temp = pd.DataFrame(data)
                df = pd.concat([df,temp],axis=0,ignore_index=True)
                start += time_step
                if start >= end:
                    break
            except Exception as e:
                print(f"Error comes from not in gateio: {e}")
                return []
            if attempts == max_attempts:
                print(f"Max attempts reached for {coinbase_symbol}")
                break
        return df
    else:
        df = []
        print(f"{coinbase_symbol} not in coinbase")
        return df
    
async def get_kraken_klines(session,base_asset,quote_asset,start,end):
    """ 
    kraken kline api
    https://docs.kraken.com/rest/#tag/Market-Data/operation/getOHLCData
    """
    if base_asset =='BTC':
        kraken_base_asset = 'XBT'
    else:
        kraken_base_asset = base_asset
    if quote_asset == 'BTC':
        kraken_quote_asset = 'XBT'
    else:
        kraken_quote_asset = quote_asset
    symbol = kraken_base_asset+kraken_quote_asset
    if symbol in kraken_symbols:
        url = f"https://iapi.kraken.com/api/internal/markets/{base_asset}/{quote_asset}/ticker/history?since={int(start/1000)}&interval=1&version=2&count=5000"
        headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Referer":"https://pro.kraken.com/"}
        try:
            data = await fetch(session, url,headers=headers)
            df = pd.DataFrame(data['result']['data'])
            return df
        except Exception as e:
            print(f"Error comes from not in kraken: {e}")
            return []
    else:
        df = []
        print(f"{symbol} not in kraken")
        return df

async def get_bitget_klines(session,base_asset,quote_asset,start,end):
    """ 
    bitget kline api
    https://www.bitget.com/api-doc/spot/market/Get-Candle-Data
    """
    time_step = 1000 * 60 * 200  # 对于Hitbtc
    symbol = base_asset + quote_asset

    if symbol in bitget_symbols:
        df = pd.DataFrame()
        attempts = 1
        max_attempts = 5
        while True:
            url = f"https://api.bitget.com/api/v2/spot/market/candles?symbol={symbol}&granularity=1min&startTime={start}&endTime={start+time_step}&limit=1000"
            try:
                data = await fetch(session, url)
                temp = pd.DataFrame(data['data'])
                df = pd.concat([df,temp],axis=0,ignore_index=True)
                start = start+ time_step
                attempts = 1
                if start >= end:
                    break 
                
            except Exception as e:
                print(f"Error comes from not in bitget: {e}")
                return []
            if attempts == max_attempts:
                print(f"Max attempts reached for {symbol}")
                break
        return df
    else:
        df = []
        print(f"{symbol} not in hitbtc")
        return df
    
exchange_url =  "https://fapi.binance.com/fapi/v1/exchangeInfo"
symbols = [{'base_asset':x['baseAsset'],'quote_asset':x['quoteAsset']} for x in requests.get(exchange_url).json()['symbols'] if x['status'] == 'TRADING']

IndexComponents_url = f"https://www.binance.com/fapi/v1/constituents?symbol={base_asset+quote_asset}"
resp = requests.get(IndexComponents_url).json().get('constituents')
resp = [x['exchange'] for x in resp]

# 将 threshold 设置为 0.03 
threshold = 0.003

async def main():
    async with aiohttp.ClientSession() as session:
        # 启动多个异步任务
        tasks = [
            get_binance_klines(session, base_asset,quote_asset, start, end),
            get_okx_klines(session, base_asset,quote_asset, start, end),
            get_bybit_klines(session, base_asset,quote_asset, start, end),
            get_huobi_klines(session, base_asset,quote_asset),
            get_hitbtc_klines(session, base_asset,quote_asset, start, end),
            get_gateio_klines(session, base_asset,quote_asset, start, end),
            get_bitmax_klines(session, base_asset,quote_asset, start, end),
            get_kucoin_kliens(session, base_asset,quote_asset, start, end),
            get_mexc_klines(session, base_asset,quote_asset, start, end),
            get_coinbase_klines(session, base_asset,quote_asset, start, end),
            get_kraken_klines(session, base_asset,quote_asset, start, end),
            get_bitget_klines(session, base_asset,quote_asset, start, end),
            get_binanceIndex_klines(session, base_asset,quote_asset, start, end)
        ]
        # 等待所有异步任务完成
        results = await asyncio.gather(*tasks)
        return (results)
   
loop = asyncio.get_event_loop()

# If the loop is already running, use create_task and await
if loop.is_running():
    task = loop.create_task(main())
    result = await task  # This will give you the returned value from main()
else:
    # If the loop is not running, use run_until_complete
    result = loop.run_until_complete(main())  # This will also give you the returned value

if len(result[0])>0:
    binance_p = result[0].iloc[:,:6]
    binance_p.columns = ['openTime','open','high','low','close','vol']
    binance_p['openTime'] = pd.to_datetime(binance_p['openTime'],unit='ms')
    binance_p[['open','high','low','close','vol']] = binance_p[['open','high','low','close','vol']].astype(float)
    binance_p.sort_values(by='openTime',ascending=True,inplace=True)
    binance_p.insert(0,'symbol',base_asset+quote_asset)
    binance_p.insert(1,'exchange','binance')
    components_check(binance_p,resp)
else:
    binance_p = pd.DataFrame()
if len(result[1])>0:
    okx_p = result[1]
    okx_p = sorting(okx_p)
    okx_p.insert(0,'symbol',base_asset+quote_asset)
    okx_p.insert(1,'exchange','okex')
    components_check(okx_p,resp)

else:
    okx_p = pd.DataFrame()
if len(result[2])>0:
    bybit_p = result[2]
    bybit_p  = sorting(bybit_p)
    bybit_p.insert(0,'symbol',base_asset+quote_asset)
    bybit_p.insert(1,'exchange','huobi')
    components_check(bybit_p,resp)
else:
    bybit_p = pd.DataFrame()
if len(result[3])>0:
    huobi_p = result[3]
    huobi_p = sorting(huobi_p)
    huobi_p.insert(0,'symbol',base_asset+quote_asset)
    huobi_p.insert(1,'exchange','huobi')
    components_check(huobi_p,resp)
    huobi_p = huobi_p[huobi_p['openTime']>=pd.to_datetime(start,unit='ms')]
else:
    huobi_p = pd.DataFrame()
if len(result[4])>0:
    hitbtc_p = result[4]
    hitbtc_p['timestamp'] = hitbtc_p['timestamp'].str.replace("T"," ").str.split(".",expand=True)[0]
    hitbtc_p['timestamp'] = pd.to_datetime(hitbtc_p['timestamp'])
    hitbtc_p.drop(columns = 'volume_quote',inplace=True)
    hitbtc_p.columns = ['openTime','open','high','low','close','vol']
    hitbtc_p[['open','high','low','close','vol']] = hitbtc_p[['open','high','low','close','vol']].astype(float)
    hitbtc_p.insert(0,'symbol',base_asset+quote_asset)
    hitbtc_p.insert(1,'exchange','hitbtc')
    hitbtc_p.dropna(inplace=True)
    components_check(hitbtc_p,resp)
else:
    hitbtc_p = pd.DataFrame()
if len(result[5])>0:
    gateio_p = result[5].iloc[:,[0,2,3,4,5,6]]
    gateio_p.columns = ['openTime','close','high','low','open','vol']
    gateio_p['openTime'] = pd.to_datetime(gateio_p['openTime'].astype(int),unit='s')
    gateio_p[['close','high','low','open','vol']] = gateio_p[['close','high','low','open','vol']].astype(float)
    gateio_p.sort_values(by="openTime",ascending=True,inplace=True)
    gateio_p.insert(0,'symbol',base_asset+quote_asset)
    gateio_p.insert(1,'exchange','gateio')
    components_check(gateio_p,resp)
else:
    gateio_p = pd.DataFrame()
if len(result[6])>0:
    bitmax_p = result[6]
    bitmax_p = sorting(bitmax_p)
    bitmax_p.insert(0,'symbol',base_asset+quote_asset)
    bitmax_p.insert(1,'exchange','bitmax')
    components_check(bitmax_p,resp)
else:
    bitmax_p = pd.DataFrame()
if len(result[7])>0:
    kucoin_p = result[7]
    kucoin_p = sorting(kucoin_p)
    kucoin_p.insert(0,'symbol',base_asset+quote_asset)
    kucoin_p.insert(1,'exchange','kucoin')
    components_check(kucoin_p,resp)
else:
    kucoin_p = pd.DataFrame()
if len(result[8])>0:
    mexc_p = result[8]
    mexc_p = sorting(mexc_p)
    mexc_p.insert(0,'symbol',base_asset+quote_asset)
    mexc_p.insert(1,'exchange','mexc')
    components_check(mexc_p,resp)
else:
    mexc_p = pd.DataFrame()
if len(result[9])>0:
    coinbase_p = result[9]
    coinbase_p = sorting(coinbase_p)
    coinbase_p.insert(0,'symbol',base_asset+quote_asset)
    coinbase_p.insert(1,'exchange','coinbase')
    components_check(coinbase_p,resp)
else:
    coinbase_p = pd.DataFrame()
if len(result[10])>0:
    kraken_p = result[10]
    kraken_p = kraken_p.iloc[:,[0,1,2,3,4,6]]
    kraken_p.columns = ['openTime','open','high','low','close','vol']
    kraken_p['openTime'] = pd.to_datetime(kraken_p['openTime'],unit='s')
    kraken_p[['open','high','low','close','vol']] = kraken_p[['open','high','low','close','vol']].astype(float)
    kraken_p = kraken_p[kraken_p['openTime'] >= pd.to_datetime(start,unit='ms')]
    kraken_p.insert(0,'symbol',base_asset+quote_asset)
    kraken_p.insert(1,'exchange','kraken')
    components_check(kraken_p,resp)
else:
    kraken_p = pd.DataFrame()
if len(result[11])>0:
    bitget_p = result[11]
    bitget_p = sorting(bitget_p)
    bitget_p.insert(0,'symbol',base_asset+quote_asset)
    bitget_p.insert(1,'exchange','bitget')
    components_check(bitget_p,resp)
else:
    bitget_p = pd.DataFrame()
if len(result[12])>0:
    binanceIndex_p = result[12]
    binanceIndex_p = sorting(binanceIndex_p)
    binanceIndex_p.insert(0,'symbol',base_asset+quote_asset)
    binanceIndex_p.insert(1,'exchange','binanceIndex')
    components_check(binanceIndex_p,resp)
else:
    binanceIndex_p = pd.DataFrame()

# 分析
legend_labels = []
legend_labels.append('binance_indexPrice')
legend_labels.append('openTime')
variable_mapping = {'binance':binance_p,'okx':okx_p,'huobi':huobi_p,'coinbase':coinbase_p,
                    'hitbtc':hitbtc_p,'gate.io':gateio_p,'bitmax':bitmax_p,'bybit':bybit_p,'kucoin':kucoin_p,'mexc':mexc_p,
                    'bitget':bitget_p,'kraken':kraken_p}
volume_data = pd.DataFrame(columns = ['exchange','vol'])
get_frozen_kline = dict()
listed = pd.DataFrame(columns = ['exchange','effective'])

merged_price = pd.DataFrame()
merged_price['openTime'] = binanceIndex_p['openTime']
# 使用merge 将close 价格拼接
merged_price = pd.merge(merged_price,binanceIndex_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'binance_indexPrice'})
if len(binance_p)>0:
    legend_labels.append('binance')
    volume_data.loc[0] = ['binance',binance_p['vol'].sum()]
    binance_p = pd.merge(left=binanceIndex_p,right=binance_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x',''))[['openTime','symbol','open',
                                                                                                                                      'high','low','close','vol','effective']]
    get_frozen_kline['binance'] = binance_p['close'].isna().sum() + binance_p.apply(frozen_kline,axis=1).sum()
    merged_price  = pd.merge(merged_price,binance_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'binance'})
    binance_dic = {"exchange":'binance','effective':binance_p['effective'][0]}
    listed = pd.concat([listed,pd.DataFrame(binance_dic,index=[0])],axis=0,ignore_index=True)

if len(okx_p)>0:
    legend_labels.append('okx')
    volume_data.loc[1] = ['okx',okx_p['vol'].sum()]
    okx_p = pd.merge(left=binanceIndex_p,right=okx_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x',''))[['openTime','symbol','open',
                                                                                                                                      'high','low','close','vol','effective']]
    get_frozen_kline['okx'] = okx_p['close'].isna().sum() + okx_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,okx_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'okx'})
    okx_dic = {"exchange":'okx','effective':okx_p.dropna(subset=['effective']).reset_index()['effective'][0]}
    listed = pd.concat([listed,pd.DataFrame(okx_dic,index=[0])],axis=0,ignore_index=True)


if len(huobi_p)>0:
    legend_labels.append('huobi')
    volume_data.loc[2] = ['huobi',huobi_p['vol'].sum()]
    huobi_p = pd.merge(left=binanceIndex_p,right=huobi_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x',''))[['openTime','symbol','open',
                                                                                                                                      'high','low','close','vol','effective']]
    get_frozen_kline['huobi'] = huobi_p['close'].isna().sum() + huobi_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,huobi_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'huobi'})
    huobi_dic = {"exchange":'huobi','effective':huobi_p['effective'][0]}
    listed = pd.concat([listed,pd.DataFrame(huobi_dic,index=[0])],axis=0,ignore_index=True)
  

if len(coinbase_p)>0:
    legend_labels.append('coinbase')
    volume_data.loc[3] = ['coinbase',coinbase_p['vol'].sum()]
    coinbase_p = pd.merge(left=binanceIndex_p,right=coinbase_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x',''))[['openTime','symbol','open',
                                                                                                                                      'high','low','close','vol','effective']]
    get_frozen_kline['coinbase'] = coinbase_p['close'].isna().sum() + coinbase_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,coinbase_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'coinbase'})
    coinbase_dic = {"exchange":'coinbase','effective':coinbase_p['effective'][0]}
    listed = pd.concat([listed,pd.DataFrame(coinbase_dic,index=[0])],axis=0,ignore_index=True)
   

if len(hitbtc_p)>0:
    legend_labels.append('hitbtc')
    volume_data.loc[4] = ['hitbtc',hitbtc_p['vol'].sum()]
    hitbtc_p = pd.merge(left=binanceIndex_p,right=hitbtc_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x',''))[['openTime','symbol','open',
                                                                                                                                      'high','low','close','vol','effective']]
    get_frozen_kline['hitbtc'] = hitbtc_p['close'].isna().sum() + hitbtc_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,hitbtc_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'hitbtc'})
    if hitbtc_p['effective'][0] is np.nan:
        hitbtc_dic = {"exchange":'hitbtc','effective':False}
    else:
        hitbtc_dic = {"exchange":'hitbtc','effective':hitbtc_p['effective'][0]}
    listed = pd.concat([listed,pd.DataFrame(hitbtc_dic,index=[0])],axis=0,ignore_index=True)
  

if len(gateio_p)>0:
    legend_labels.append('gate.io')
    volume_data.loc[5] = ['gate.io',gateio_p['vol'].sum()]
    gateio_p = pd.merge(left=binanceIndex_p,right=gateio_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x',''))[['openTime','symbol','open',
                                                                                                                                      'high','low','close','vol','effective']]
    get_frozen_kline['gate.io'] = gateio_p['close'].isna().sum() + gateio_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,gateio_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'gate.io'})
    if gateio_p['effective'][0] is np.nan:
        gateio_dic = {"exchange":'gate.io','effective':False}
    else:
        gateio_dic = {"exchange":'gate.io','effective':gateio_p['effective'][0]}
    listed = pd.concat([listed,pd.DataFrame(gateio_dic,index=[0])],axis=0,ignore_index=True)


if len(bitmax_p)>0:
    legend_labels.append('bitmax')
    volume_data.loc[6] = ['bitmax',bitmax_p['vol'].sum()]
    bitmax_p = pd.merge(left=binanceIndex_p,right=bitmax_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x',''))[['openTime','symbol','open',
                                                                                                                                      'high','low','close','vol','effective']]
    get_frozen_kline['bitmax'] = bitmax_p['close'].isna().sum() + bitmax_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,bitmax_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'bitmax'})
    if bitmax_p['effective'][0] is np.nan:
        bitmax_dic = {"exchange":'bitmax','effective':False}
    else:
        bitmax_dic = {"exchange":'bitmax','effective':bitmax_p['effective'][0]}
    listed = pd.concat([listed,pd.DataFrame(bitmax_dic,index=[0])],axis=0,ignore_index=True)


if len(bybit_p)>0:
    legend_labels.append('bybit')
    volume_data.loc[7] = ['bybit',bybit_p['vol'].sum()]
    bybit_p = pd.merge(left=binanceIndex_p,right=bybit_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x',''))[['openTime','symbol','open',
                                                                                                                                      'high','low','close','vol','effective']]
    get_frozen_kline['bybit'] = bybit_p['close'].isna().sum() + bybit_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,bybit_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'bybit'})
    if bybit_p['effective'][0] is np.nan:
        bybit_dic = {"exchange":'bybit','effective':False}
    else:
        bybit_dic = {"exchange":'bybit','effective':bybit_p['effective'][0]}
    listed = pd.concat([listed,pd.DataFrame(bybit_dic,index=[0])],axis=0,ignore_index=True)
    

if len(kucoin_p)>0:
    legend_labels.append('kucoin')
    volume_data.loc[8] = ['kucoin',kucoin_p['vol'].sum()]
    kucoin_p = pd.merge(left=binanceIndex_p,right=kucoin_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x',''))[['openTime','symbol','open',
                                                                                                                                      'high','low','close','vol','effective']]
    get_frozen_kline['kucoin'] = kucoin_p['close'].isna().sum() + kucoin_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,kucoin_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'kucoin'})
    if kucoin_p['effective'][0] is np.nan:
        kucoin_dic = {"exchange":'kucoin','effective':False}
    else:
        kucoin_dic = {"exchange":'kucoin','effective':kucoin_p['effective'][0]}
    listed = pd.concat([listed,pd.DataFrame(kucoin_dic,index=[0])],axis=0,ignore_index=True)


if len(mexc_p)>0:
    legend_labels.append('mexc')
    volume_data.loc[9] = ['mexc',mexc_p['vol'].sum()]
    mexc_p = pd.merge(left=binanceIndex_p,right=mexc_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x',''))[['openTime','symbol','open',
                                                                                                                                      'high','low','close','vol','effective']]
    get_frozen_kline['mexc'] = mexc_p['close'].isna().sum() + mexc_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,mexc_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'mexc'})
    if mexc_p['effective'][0] is np.nan:
        mexc_dic = {"exchange":'mexc','effective':False}
    else:
        mexc_dic = {"exchange":'mexc','effective':mexc_p['effective'][0]}
    listed = pd.concat([listed,pd.DataFrame(mexc_dic,index=[0])],axis=0,ignore_index=True)
    
if len(bitget_p)>0:
    legend_labels.append('bitget')
    volume_data.loc[10] = ['bitget',bitget_p['vol'].sum()]
    bitget_p = pd.merge(left=binanceIndex_p,right=bitget_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x',''))[['openTime','symbol','open',
                                                                                                                                      'high','low','close','vol','effective']]
    get_frozen_kline['bitget'] = bitget_p['close'].isna().sum() + bitget_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,bitget_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'bitget'})
    if bitget_p['effective'][0] is np.nan:
        bitget_dic = {"exchange":'bitget','effective':False}
    else:
        bitget_dic = {"exchange":'bitget','effective':bitget_p['effective'][0]}
    listed = pd.concat([listed,pd.DataFrame(bitget_dic,index=[0])],axis=0,ignore_index=True)

if len(kraken_p)>0:
    legend_labels.append('kraken')
    volume_data.loc[11] = ['kraken',kraken_p['vol'].sum()]
    kraken_p = pd.merge(left=binanceIndex_p,right=kraken_p,left_on="openTime",right_on='openTime',how='left',suffixes = ('_x',''))[['openTime','symbol','open',
                                                                                                                                      'high','low','close','vol','effective']]
    get_frozen_kline['kraken'] = kraken_p['close'].isna().sum() + kraken_p.apply(frozen_kline,axis=1).sum()
    merged_price = pd.merge(merged_price,kraken_p[['openTime','close']],on='openTime',how='left').rename(columns={'close':'kraken'})
    if kraken_p['effective'][0] is np.nan:
        kraken_dic = {"exchange":'kraken','effective':False}
    else:
        kraken_dic = {"exchange":'kraken','effective':kraken_p['effective'][0]}
    listed = pd.concat([listed,pd.DataFrame(kraken_dic,index=[0])],axis=0,ignore_index=True)

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

diff_df =  merged_price.iloc[:, 1:].subtract(merged_price['binance_indexPrice'], axis=0)
std_devs = diff_df.std()
merged_price.loc['std_dev'] = std_devs

result = merged_price.iloc[-1,1:].to_frame()
result['std_dev'] = result['std_dev'].astype(float) 
result.insert(0,'symbol',base_asset+quote_asset)
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
result.insert(0,'symbol',base_asset+quote_asset)

def vol_ratio(row):
    return round(row['vol']/result['vol'].sum()*100,2)
result['volPercent'] = result.apply(vol_ratio,axis=1)
fig = plt.figure(figsize=(20, 9))
gs_main = GridSpec(2, 2, figure=fig)  # 修改为2行1列

# 第一行第一列:线图 
ax1 = fig.add_subplot(gs_main[0,:])
ax1.plot(merged_price.iloc[:-1,1:])
ax1.set_title(f"{base_asset+quote_asset} Price Comparison (from {ts_time(start)} to {ts_time(end)})")
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
result = pd.merge(left=result,right=listed,left_on='exchange',right_on='exchange')
result


