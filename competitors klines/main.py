import aiohttp
import asyncio
import pandas as pd 
import time 
import datetime as dt 
import time 
import pytz 
import requests 
import numpy as np 
import math
import nest_asyncio
nest_asyncio.apply()
import warnings 
import Functions as f 
warnings.filterwarnings('ignore') 

def main():
    backtrack_hours = 24
    start = int(time.time() * 1000) - 1000 * 60 * 60 * backtrack_hours
    end = int(time.time() * 1000) - 1000 * 60 
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
    if __name__ == "__main__":
        results = asyncio.run(get_exchange_info())

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
    
            try:
                url = f"https://fapi.binance.com/fapi/v1/indexPriceKlines?pair={symbol}&interval=1m&startTime={start}&endTime={end}&limit=1500"
                data = await fetch(session, url)
                temp = pd.DataFrame(data).iloc[:, :6]
                df = pd.concat([df, temp], axis=0, ignore_index=True)
            except Exception as e:
                print(f"Error comes from not in binanceIndex, {attempts} times request: {e}",url)
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
            print(f"{symbol} not in okex")
            return df 

    async def get_bybit_klines(session, base_asset,quote_asset, start, end):
        """ 
        Bybit Kline API
        https://bybit-exchange.github.io/docs/inverse/#t-querykline
        """
        time_step = 1000 * 60 * 1000  # 对于Bybit
        if base_asset == 'BEAMX':
            base_asset == 'BEAM'
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
            print(f"{symbol} not in bybit")
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
            print(f"{symbol} not in gateio")
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
        if base_asset == 'BEAMX':
            base_asset == 'BEAM'
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
            print(f"{symbol} not in bitget")
            return df

    exchange_url =  "https://fapi.binance.com/fapi/v1/exchangeInfo"
    symbols = [{'base_asset':x['baseAsset'],'quote_asset':x['quoteAsset']} for x in requests.get(exchange_url).json()['symbols'] if x['status'] == 'TRADING' and x['contractType']=='PERPETUAL'
                                                                                                                    and x['underlyingType']=='COIN']

    merged_containers = pd.DataFrame()
    for i in range(len(symbols)):
        if "1000" in symbols[i]:
            base_asset = symbols[i].get('base_asset').replace('1000',"")
        else:
            base_asset = symbols[i].get('base_asset')
        quote_asset = symbols[i].get('quote_asset')
        print(f"开始获取{base_asset+quote_asset}的数据")

        # IndexComponents_url = f"https://www.binance.com/fapi/v1/constituents?symbol={base_asset+quote_asset}"
        # try:
        #     resp = requests.get(IndexComponents_url).json().get('constituents')
        #     resp = [x['exchange'] for x in resp]
        # except Exception as e:
        #     print(f"Error comes from constituents endpoint: {requests.get(IndexComponents_url).text}")

        symbol = base_asset + quote_asset
        cross = f.binance_cross_symbols()

        cross_symbols = cross.get_binance_CrossSymbols(symbol = symbol,original_start=  start, end= end )
        if len(cross_symbols) >0:
            cross_symbols = cross_symbols.iloc[:,[0,1,2,3,13,14,15,16,8]]
            condition = cross_symbols['componentSymbol'].isin(cross_symbols.groupby('componentSymbol').vol.sum().sort_values(ascending=False).nlargest(3).index.tolist())
            cross_symbols = cross_symbols[condition]
            cross_symbols_symbol = cross_symbols['componentSymbol'].unique().tolist()
            
           
            # print(symbol)
            # print(cross_symbols_symbol)
            if len(cross_symbols_symbol) == 3:
                cross_1 = cross_symbols.groupby('componentSymbol').get_group(cross_symbols_symbol[0])
                cross_2 = cross_symbols.groupby('componentSymbol').get_group(cross_symbols_symbol[1])
                cross_3 = cross_symbols.groupby('componentSymbol').get_group(cross_symbols_symbol[2])
                merged_containers = pd.concat([merged_containers,cross_1,cross_2,cross_3],axis=0,ignore_index=True)

            elif len(cross_symbols_symbol) == 2:
                cross_1 = cross_symbols.groupby('componentSymbol').get_group(cross_symbols_symbol[0])
                cross_2 = cross_symbols.groupby('componentSymbol').get_group(cross_symbols_symbol[1])
                cross_3 = pd.DataFrame()
                merged_containers = pd.concat([merged_containers,cross_1,cross_2],axis=0,ignore_index=True)

            elif len(cross_symbols_symbol) == 1:
                cross_1 = cross_symbols.groupby('componentSymbol').get_group(cross_symbols_symbol[0])
                cross_2 = pd.DataFrame()
                cross_3 = pd.DataFrame()
                merged_containers = pd.concat([merged_containers,cross_1],axis=0,ignore_index=True)
            else:
                cross_1 = pd.DataFrame()
                cross_2 = pd.DataFrame()
                cross_3 = pd.DataFrame()

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
               
                results = await asyncio.gather(*tasks)
                return (results)
    
        if __name__ == "__main__":
            result = asyncio.run(main())
            
        if len(result[0])>0:
            binance_p = result[0]
            binance_p = sorting(binance_p) 
            binance_p.insert(0,'symbol',base_asset+quote_asset)
            binance_p.insert(1,'exchange','binance')
            binance_p.insert(2,'componentSymbol',base_asset+quote_asset)
            # components_check(binance_p,resp)
        else:
            binance_p = pd.DataFrame()

        if len(result[1])>0:
            okx_p = result[1]
            okx_p = sorting(okx_p)
            okx_p.insert(0,'symbol',base_asset+quote_asset)
            okx_p.insert(1,'exchange','okex')
            okx_p.insert(2,'componentSymbol',base_asset+'-'+quote_asset)
            # components_check(okx_p,resp)

        else:
            okx_p = pd.DataFrame()
        if len(result[2])>0:
            bybit_p = result[2]
            bybit_p = sorting(bybit_p)
            bybit_p.insert(0,'symbol',base_asset+quote_asset)
            bybit_p.insert(1,'exchange','bybit')
            bybit_p.insert(2,'componentSymbol',base_asset+quote_asset)
            # components_check(bybit_p,resp)
        else:
            bybit_p = pd.DataFrame()

        if len(result[3])>0:
            huobi_p = result[3]
            huobi_p = sorting(huobi_p)
            huobi_p.insert(0,'symbol',base_asset+quote_asset)
            huobi_p.insert(1,'exchange','huobi')
            huobi_p.insert(2,'componentSymbol',base_asset.lower()+quote_asset.lower())
            # components_check(huobi_p,resp)
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
            hitbtc_p.insert(2,'componentSymbol',base_asset+quote_asset)
            # components_check(hitbtc_p,resp)
        else:
            hitbtc_p = pd.DataFrame()

        if len(result[5])>0:
            gateio_p = result[5].iloc[:,[0,5,3,4,2,6]]
            gateio_p.columns = ['openTime','open','high','low','close','vol']
            gateio_p['openTime'] = pd.to_datetime(gateio_p['openTime'],unit='s')
            gateio_p[['open','high','low','close','vol']] = gateio_p[['open','high','low','close','vol']].astype(float)
            gateio_p.insert(0,'symbol',base_asset+quote_asset)
            gateio_p.insert(1,'exchange','gateio')
            gateio_p.insert(2,'componentSymbol',base_asset+'_'+quote_asset)
            # components_check(gateio_p,resp)
        else:
            gateio_p = pd.DataFrame()

        if len(result[6])>0:
            bitmax_p = result[6]
            bitmax_p = sorting(bitmax_p)
            bitmax_p.insert(0,'symbol',base_asset+quote_asset)
            bitmax_p.insert(1,'exchange','bitmax')
            bitmax_p.insert(2,'componentSymbol',base_asset+'/'+quote_asset)
            # components_check(bitmax_p,resp)
        else:
            bitmax_p = pd.DataFrame()

        if len(result[7])>0:
            kucoin_p = result[7]
            kucoin_p = sorting(kucoin_p)
            kucoin_p.insert(0,'symbol',base_asset+quote_asset)
            kucoin_p.insert(1,'exchange','kucoin')
            kucoin_p.insert(2,'componentSymbol',base_asset+'-'+quote_asset)
            # components_check(kucoin_p,resp)
        else:
            kucoin_p = pd.DataFrame()

        if len(result[8])>0:
            mexc_p = result[8]
            mexc_p = sorting(mexc_p)
            mexc_p.insert(0,'symbol',base_asset+quote_asset)
            mexc_p.insert(1,'exchange','mexc')
            mexc_p.insert(2,'componentSymbol',base_asset+quote_asset)
            # components_check(mexc_p,resp)
        else:
            mexc_p = pd.DataFrame()

        if len(result[9])>0:
            coinbase_p = result[9]
            coinbase_p = sorting(coinbase_p)
            coinbase_p.insert(0,'symbol',base_asset+quote_asset)
            coinbase_p.insert(1,'exchange','coinbase.pro')
            coinbase_p.insert(2,'componentSymbol',base_asset+'-USD')
            # components_check(coinbase_p,resp)
        else:
            coinbase_p = pd.DataFrame()

        if len(result[10])>0:
            kraken_p = result[10]
            kraken_p = sorting(kraken_p)
            kraken_p.insert(0,'symbol',base_asset+quote_asset)
            kraken_p.insert(1,'exchange','kraken')
            kraken_p.insert(2,'componentSymbol',base_asset+'/'+quote_asset)
            # components_check(kraken_p,resp)
        else:
            kraken_p = pd.DataFrame()

        if len(result[11])>0:
            bitget_p = result[11]
            bitget_p = sorting(bitget_p)
            bitget_p.insert(0,'symbol',base_asset+quote_asset)
            bitget_p.insert(1,'exchange','bitget')
            bitget_p.insert(2,'componentSymbol',base_asset+quote_asset)
            # components_check(bitget_p,resp)
        else:
            bitget_p = pd.DataFrame()

        if len(result[12])>0:
            binanceIndex_p = result[12]
            binanceIndex_p = sorting(binanceIndex_p)
            binanceIndex_p.insert(0,'symbol',base_asset+quote_asset)
            binanceIndex_p.insert(1,'exchange','binanceIndex')
            # components_check(binanceIndex_p,resp)
        else:
            binanceIndex_p = pd.DataFrame()

        exchanges_list = [merged_containers,binance_p,okx_p,bybit_p,huobi_p,hitbtc_p,gateio_p,bitmax_p,kucoin_p,mexc_p,coinbase_p,kraken_p,bitget_p,binanceIndex_p]
        merged_containers = pd.concat(exchanges_list,axis=0,ignore_index=True)
        merged_containers.drop_duplicates(inplace=True)

    print(merged_containers)
    return merged_containers

if __name__ == "__main__":
    main()

