import pandas as pd 
import requests 

class binance_cross_symbols:
    def __init__(self):
        self.binance_spot_symbols = self.get_binance_spot_symbols()
        self.quote = self.quote_asset()
        self.exchangeInfo = self.exchangeInfo()

    def exchangeInfo(self):
        """ 
        get binance exchange info 
        """
        url = "https://api.binance.com/api/v3/exchangeInfo"
        exchangeInfo = requests.get(url).json()
        return exchangeInfo

    def get_binance_spot_symbols(self):
        """ 
        get binance spot symbols 
        """
        url = "https://api.binance.com/api/v3/exchangeInfo"
        spot_symbols = [x['symbol'] for x in requests.get(url).json()['symbols'] if x['status']=='TRADING']
        return spot_symbols
    
    def quote_asset(self):
        url = "https://api.binance.com/api/v3/exchangeInfo"
        quote = list(set([x['quoteAsset'] for x in requests.get(url).json()['symbols'] if x['status']=='TRADING']))
        return quote

    # 通过symbol 拆分base asset 和quote asset 
    def split_asset(self,symbol):
        """ 
        split symbol into base asset and quote asset 
        """
        
        for i in self.quote:
            if symbol.endswith(i):
                asset = symbol.split(i)
                if len(asset) == 2 and len(asset[0])>0:
                    return asset[0],i
                elif len(asset)==2 and len(asset[0]) == 0:
                    resplit_asset = symbol.split(asset[1])
                    return resplit_asset[0],asset[1]
            else:
                continue

    # 获取最佳的3个 cross symbol 
    def get_cross_symbol(self,symbol):
        """ 
        input symbol, return cross symbol list 
        """
        base,quote = self.split_asset(symbol)
        cross_symbol = []
        resp = [x['symbol'] for x in self.exchangeInfo['symbols'] if x['baseAsset']==base and x['status']=='TRADING']

        if len(resp)>0:
            if symbol in resp:
                resp.remove(symbol)
                for i in resp:
                    temp_list = []
                    try:
                        cross_base,cross_quote = self.split_asset(i)
                    except Exception as e:
                        print(i,e)
                        continue

                    cross_1st = i
                    cross_2nd = [x['symbol'] for x in self.exchangeInfo['symbols'] if (x['symbol']==quote+cross_quote and x['status']=='TRADING') or (x['symbol']==cross_quote+quote and x['status']=='TRADING')]
                    temp_list.append(cross_1st)
                    if len(cross_2nd) >0:
                        temp_list.append(cross_2nd[0])
                    cross_symbol.append(temp_list)
                    cross_symbol = [x for x in cross_symbol if len(x)==2]
        return cross_symbol 

    def get_binance_CrossSymbols(self,symbol,original_start,end):
        """ 
        get vol top 3 binance cross symbols klines 
        """
        cross_symbols = pd.DataFrame()
        symbols = self.get_cross_symbol(symbol)
        time_step = 1000 * 60 * 1000 
        if symbol in self.binance_spot_symbols:
            price_url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&startTime={original_start}&endTime={original_start+time_step}&limit=1000"
            try: 
                price = requests.get(price_url).json()[0][2]
                if len(symbols) >0:
                    for cross in symbols:
                        start = original_start 
                        while True:
                            cross_1st_url = f"https://api.binance.com/api/v3/klines?symbol={cross[0]}&interval=1m&startTime={start}&endTime={start+time_step}&limit=1000"
                            cross_2nd_url = f"https://api.binance.com/api/v3/klines?symbol={cross[1]}&interval=1m&startTime={start}&endTime={start+time_step}&limit=1000"

                            cross_1st = requests.get(cross_1st_url).json()
                            if len(cross_1st) ==0:
                                break 
                            cross_2nd = requests.get(cross_2nd_url).json()
                            cross_1st = pd.DataFrame(cross_1st).iloc[:,:6]
                            cross_2nd = pd.DataFrame(cross_2nd).iloc[:,:5]
                            cross_1st.rename(columns={0:'openTime',1:"cross_1_open",2:"cross_1_high",3:"cross_1_low",4:"cross_1_close",5:'vol'},inplace=True)
                            cross_2nd.rename(columns={0:'openTime',1:"cross_2_open",2:"cross_2_high",3:"cross_2_low",4:"cross_2_close"},inplace=True)
                            cross_1st['openTime'] = pd.to_datetime(cross_1st['openTime'],unit='ms')
                            cross_2nd['openTime'] = pd.to_datetime(cross_2nd['openTime'],unit='ms')
                            cross_1st[cross_1st.columns[1:]] = cross_1st[cross_1st.columns[1:]].astype(float) 
                            cross_2nd[cross_2nd.columns[1:]] = cross_2nd[cross_2nd.columns[1:]].astype(float) 
                            cross_merge = pd.merge(cross_1st,cross_2nd,on='openTime',how='inner') 
                            if cross_1st.at[0,'cross_1_high'] * cross_2nd.at[0,'cross_2_high'] < (2*abs(float(price))):
                                
                                cross_merge['open'] = cross_merge['cross_1_open']*cross_merge['cross_2_open']
                                cross_merge['high'] = cross_merge['cross_1_high']*cross_merge['cross_2_high']
                                cross_merge['low'] = cross_merge['cross_1_low']*cross_merge['cross_2_low']
                                cross_merge['close'] = cross_merge['cross_1_close']*cross_merge['cross_2_close']
                                cross_merge.insert(0,'symbol',symbol)
                                cross_merge.insert(1,'exchange','binanceCross')
                                cross_merge.insert(2,'componentSymbol',cross[0]+'*'+cross[1])
                                cross_symbols = pd.concat([cross_symbols,cross_merge],ignore_index=True,axis=0)
                            else:
                                cross_merge['open'] = cross_merge['cross_1_open']/cross_merge['cross_2_open']
                                cross_merge['high'] = cross_merge['cross_1_high']/cross_merge['cross_2_high']
                                cross_merge['low'] = cross_merge['cross_1_low']/cross_merge['cross_2_low']
                                cross_merge['close'] = cross_merge['cross_1_close']/cross_merge['cross_2_close']
                                cross_merge.insert(0,'symbol',symbol)
                                cross_merge.insert(1,'exchange','binanceCross')
                                cross_merge.insert(2,'componentSymbol',cross[0]+'/'+cross[1])
                                
                                cross_symbols = pd.concat([cross_symbols,cross_merge],ignore_index=True,axis=0)
                            
                            start = start + time_step
                            if start > end:
                                break
                    return cross_symbols
                else:
                    print(f"can't find cross symbols for {symbol}")
                    cross_symbols = []
                    return cross_symbols
            except Exception as e:
                print(e)
                cross_symbols = []
                return cross_symbols      
                
        else:
            print(f"can't find cross symbols for {symbol}")
            cross_symbols = []
            return cross_symbols
