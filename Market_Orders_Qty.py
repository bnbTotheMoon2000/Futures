import pandas as pd 
import requests 

def main():
    # get exchange info data, all UM trading symbols
    exchangeInfo_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    exchangeInfo = [x for x in requests.get(exchangeInfo_url).json()['symbols'] if x['status']=='TRADING']
    mkt_cap = pd.DataFrame([{'symbol':x['symbol'],'mkt_cap':y['maxQty']} for x in exchangeInfo for y in x['filters'] if y['filterType']=='MARKET_LOT_SIZE'])
    
    result_container = []
    error_container = pd.DataFrame()
    
    for symbol in mkt_cap['symbol']:
        url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000"
    
        try:
            response = requests.request("GET", url).json()
            if len(response['bids']) != len(response['asks']):
                row_number = min(len(response['bids']),len(response['asks']))
                if len(response['bids']) < len(response['asks']):
                    response['asks'] = response['asks'][:row_number]
                else:
                    response['bids'] = response['bids'][:row_number]
            depth = pd.DataFrame(response,dtype='float')
        except Exception as e:
            print(e)
            print(url)
            print(response.json())
    
        depth[['bid_p','bid_q']]=depth.bids.apply(lambda x:pd.Series(x))
        depth[['ask_p','ask_q']]=depth.asks.apply(lambda x:pd.Series(x))
        depth.drop(columns=['bids','asks'],inplace=True)
        depth = depth.astype(float)
        depth.insert(5,'tier',[x for x in range(1,len(depth)+1)])
        depth['bidQty_cum'] = depth['bid_q'].cumsum()
        depth['askQty_cum'] = depth['ask_q'].cumsum()
        # depth = depth[['tier','bidQty_cum','askQty_cum']]
        depth.insert(0,'symbol',symbol)
    
        cap = int(mkt_cap[mkt_cap['symbol'] == symbol]['mkt_cap'].values[0])
        depth['mktCap'] = cap
        depth = depth[['symbol','E','tier','bid_p','ask_p','bid_q','ask_q','bidQty_cum','askQty_cum','mktCap']]
        # ret = depth[(depth['bidQty_cum'] <= cap) | (depth['askQty_cum'] <= cap )]
        try:
            if len(depth[(depth['bidQty_cum'] <= cap)]) >0:
                down = depth[(depth['bidQty_cum'] <= cap)].iloc[-1,:]
            else:
                down = depth.head(1)
                print(down)
            if len(depth[depth['askQty_cum'] <= cap]) >0:
                up = depth[depth['askQty_cum'] <= cap].iloc[-1,:]
            else:
                up = depth.head(1)
            down_price = up['bid_p']
            down_tier = up['tier']
            up_price = down['ask_p']
            up_tier = down['tier']
            down_agg_qty = round(depth.iat[depth[(depth['bidQty_cum'] <= cap)].iloc[-1,:].name,7],2)
            up_agg_qty = round(depth.iat[depth[(depth['askQty_cum'] <= cap)].iloc[-1,:].name,8],2)
            temp_dict = {'symbol':symbol,'price':depth.iat[0,3],'price_floor':down_price,'down_tier':down_tier,'agg_floor':down_agg_qty,'price_ceiling':up_price,'up_tier':up_tier,'agg_ceiling':up_agg_qty,'mktCap':cap}
            result_container.append(temp_dict)
        except Exception as e:
            print(url)
            print(e)
            error_container = pd.concat([error_container,depth],ignore_index=True)
    
    
    data = pd.DataFrame(result_container)
    
    data = data[['symbol','price','price_floor','down_tier','agg_floor','price_ceiling','up_tier','agg_ceiling','mktCap']]
    
    data['up_impact%'] = round(abs((data['price_ceiling'] - data['price']) /data['price'])*100,2)
    data['down_impact%'] = round(abs((data['price_floor'] - data['price']) /data['price'])*100,2)
    condi = (data['up_impact%'] >1) | (data['down_impact%']>1)
    return data[condi].sort_values(by=['up_impact%','down_impact%'],ascending=False)

if __name__ == "__main__":
    main()
