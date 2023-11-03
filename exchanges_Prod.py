import requests
import pandas as pd 
import warnings
import time 
import hmac
import hashlib
import os 
warnings.filterwarnings("ignore")

def main():
    url = "https://api-testnet.bybit.com/derivatives/v3/public/risk-limit/list"
    print("Start Downloading Bybit Data")
    response = requests.request("GET", url)
    bybit = pd.DataFrame(response.json()['result']['list'])
    bybit_symbols = list(bybit['symbol'].unique())
    # group by symbol 
    bybit.groupby(by='symbol').get_group(bybit_symbols[5])

    bybit[['limit','maintainMargin','initialMargin','isLowestRisk','maxLeverage']] = bybit[['limit','maintainMargin','initialMargin','isLowestRisk','maxLeverage']].astype(float)
    bybit.sort_values(by='maxLeverage',ascending=False)
    # bybit_50 = bybit[bybit['maxLeverage']==50.0]

    bybit[bybit['symbol'].str.endswith('USDT')]

    bybit_maxLeverage = pd.DataFrame()

    for symbol in bybit_symbols:
        single_max = bybit.groupby(by='symbol').get_group(symbol).sort_values(by='maxLeverage',ascending=False)
        bybit_maxLeverage = pd.concat([bybit_maxLeverage,single_max],ignore_index=True)

    def check_mm(df):
        if df['initialMargin'] / df['maintainMargin']>=2:
            return True
        else:
            return round(df['initialMargin'] / df['maintainMargin'],2)
        
    bybit_maxLeverage['mm_check'] = bybit_maxLeverage.apply(check_mm,axis=1)
    bybit_maxLeverage = bybit_maxLeverage[bybit_maxLeverage.symbol.str.endswith('USDT')]
    bybit_maxLeverage[bybit_maxLeverage['mm_check'] !=True]
    bybit_funding_url = "https://api2.bybit.com/contract/v5/public/support/trading-param?category=LinearPerpetual"
    funding_data = requests.request("GET",bybit_funding_url).json()['result']['list']
    funding_df = pd.DataFrame(funding_data)
    funding_df = funding_df[['symbolName','fundingRateInterval','fundingRateClamp']]
    bybit_info = pd.merge(left= bybit_maxLeverage,right=funding_df,left_on='symbol',right_on='symbolName',how='left').drop(columns=['symbolName','id'])
    bybit_info['exchange'] = 'bybit'
    print('Bybit data download completed!!!')

    # 获取OkX 所有合约交易对
    print('Start downloading OKX data')
    okx_url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    resp = requests.get(okx_url).json()['data']
    okx_symbols = []
    for symbol in resp:
        if 'USDT' in symbol['instId'] and symbol['state']=='live':
            okx_symbols.append(symbol['instId'])

    ok_bracket = pd.DataFrame()
    if len(okx_symbols)%3 ==0:
        for i in range(0,len(okx_symbols),3):
            if i%3 ==0:
                symbol_para = (okx_symbols[i].split('-SWAP')[0]+","+okx_symbols[i+1].split('-SWAP')[0]+","+okx_symbols[i+2].split('-SWAP')[0])
                okx_riskUrl = "https://www.okx.com/api/v5/public/position-tiers?instType=SWAP&tdMode=cross&uly="+symbol_para
                print(okx_riskUrl)
                resp = requests.get(okx_riskUrl).json()['data']
                temp = pd.DataFrame(resp)
                ok_bracket = pd.concat([ok_bracket,temp],ignore_index=True)
                time.sleep(1)
    else:
        for i in range(0,(len(okx_symbols)- len(okx_symbols)%3),3):
            if i%3 ==0:
                symbol_para = (okx_symbols[i].split('-SWAP')[0]+","+okx_symbols[i+1].split('-SWAP')[0]+","+okx_symbols[i+2].split('-SWAP')[0])
                okx_riskUrl = "https://www.okx.com/api/v5/public/position-tiers?instType=SWAP&tdMode=cross&uly="+symbol_para
                resp = requests.get(okx_riskUrl).json()['data']
                temp = pd.DataFrame(resp)
                ok_bracket = pd.concat([ok_bracket,temp],ignore_index=True)
                time.sleep(1)
            symbol_remaining = okx_symbols[(len(okx_symbols)%3*-1):]
            if len(symbol_remaining) == 1:
                symbol_remaining_para = symbol_remaining[0].split('-SWAP')[0]
            elif len(symbol_remaining) == 2:
                symbol_remaining_para = symbol_remaining[0].split('-SWAP')[0]+","+symbol_remaining[1].split('-SWAP')[0]
            okxRemaining_riskUrl = "https://www.okx.com/api/v5/public/position-tiers?instType=SWAP&tdMode=cross&uly="+symbol_remaining_para
            resp_remaining = requests.get(okxRemaining_riskUrl).json()['data']
            temp_remaining = pd.DataFrame(resp_remaining)
            ok_bracket = pd.concat([ok_bracket,temp_remaining],ignore_index=True)

    ok_bracket['instFamily'] = ok_bracket['instFamily'].str.replace('-',"")
    group_1 = ['BTCUSDT']
    group_2 = ["ADA", "AVAX", "BCH", "DOT", "EOS", "ETC", "ETH", "FIL", "LINK", "LTC", "TRX", "XRP"]
    group_2 = [i+'USDT' for i in group_2]
    group_2.append('BTCUSDC')

    for index,column in ok_bracket.iterrows():
        if column['instFamily'] in group_1:
            ok_bracket.loc[index,'fundingCap/Floor'] = 0.375
        elif column['instFamily'] in group_2:
            ok_bracket.loc[index,'fundingCap/Floor'] = 0.75
        else:
            ok_bracket.loc[index,'fundingCap/Floor'] = 1.5

    ok_bracket.drop(columns = 'baseMaxLoan')
    ok_bracket['funding_interval'] = 8
    ok_bracket['exchange'] ='okx'
    filter_okx = ok_bracket[['instFamily','imr','mmr','maxSz','maxLever','fundingCap/Floor','funding_interval','exchange']]

    # 获取okx合约标记价格
    # GET /api/v5/public/mark-price?instType=SWAP
    ok_mp = "https://www.okx.com/api/v5/public/mark-price?instType=SWAP"
    ok_mp = pd.DataFrame(requests.get(ok_mp).json()['data'])
    ok_mp['markPx'] = ok_mp['markPx'].astype(float)
    ok_mp['symbol'] = ok_mp['instId'].str.replace('-SWAP','').str.replace('-','')
    ok_mp['ts'] =  pd.to_datetime(ok_mp['ts'],unit='ms')
    ok_mp = ok_mp[['symbol','markPx','ts']]

    ts = int(time.time()*1000)
    url_1 = f"https://www.okx.com/priapi/v5/public/products?t={ts}&instType=SWAP&instId=BTC-USDT-SWAP,ETH-USDT-SWAP,LTC-USDT-SWAP,XRP-USDT-SWAP,BCH-USDT-SWAP,SOL-USDT-SWAP,TRB-USDT-SWAP,STARL-USDT-SWAP,PEPE-USDT-SWAP,FIL-USDT-SWAP,AIDOGE-USDT-SWAP,1INCH-USDT-SWAP,AAVE-USDT-SWAP,ADA-USDT-SWAP,AGLD-USDT-SWAP,ALGO-USDT-SWAP,ALPHA-USDT-SWAP,ANT-USDT-SWAP,APE-USDT-SWAP,API3-USDT-SWAP,APT-USDT-SWAP,AR-USDT-SWAP,ARB-USDT-SWAP,ATOM-USDT-SWAP,AVAX-USDT-SWAP,AXS-USDT-SWAP,BADGER-USDT-SWAP,BAL-USDT-SWAP,BAND-USDT-SWAP,BAT-USDT-SWAP,BICO-USDT-SWAP,BIGTIME-USDT-SWAP,BLUR-USDT-SWAP,BNB-USDT-SWAP,BNT-USDT-SWAP,BSV-USDT-SWAP,CELO-USDT-SWAP,CEL-USDT-SWAP,CETUS-USDT-SWAP,CFX-USDT-SWAP,CHZ-USDT-SWAP,COMP-USDT-SWAP,CORE-USDT-SWAP,CRO-USDT-SWAP,CRV-USDT-SWAP,CSPR-USDT-SWAP,CVC-USDT-SWAP,DASH-USDT-SWAP,DOGE-USDT-SWAP,DOT-USDT-SWAP"
    url_2 = f"https://www.okx.com/priapi/v5/public/products?t={ts}&instType=SWAP&instId=DYDX-USDT-SWAP,EGLD-USDT-SWAP,ENS-USDT-SWAP,EOS-USDT-SWAP,ETC-USDT-SWAP,ETHW-USDT-SWAP,FITFI-USDT-SWAP,FLM-USDT-SWAP,FLOKI-USDT-SWAP,FRONT-USDT-SWAP,FTM-USDT-SWAP,GALA-USDT-SWAP,GAS-USDT-SWAP,GFT-USDT-SWAP,GMT-USDT-SWAP,GMX-USDT-SWAP,GODS-USDT-SWAP,GRT-USDT-SWAP,HBAR-USDT-SWAP,ICP-USDT-SWAP,IMX-USDT-SWAP,IOST-USDT-SWAP,IOTA-USDT-SWAP,JST-USDT-SWAP,KISHU-USDT-SWAP,KLAY-USDT-SWAP,KNC-USDT-SWAP,KSM-USDT-SWAP,LDO-USDT-SWAP,LINK-USDT-SWAP,LOOKS-USDT-SWAP,LPT-USDT-SWAP,LRC-USDT-SWAP,LUNA-USDT-SWAP,LUNC-USDT-SWAP,MAGIC-USDT-SWAP,MANA-USDT-SWAP,MASK-USDT-SWAP,MATIC-USDT-SWAP,MINA-USDT-SWAP,MKR-USDT-SWAP,NEAR-USDT-SWAP,NEO-USDT-SWAP,NFT-USDT-SWAP,OMG-USDT-SWAP,ONT-USDT-SWAP,OP-USDT-SWAP,ORBS-USDT-SWAP,ORDI-USDT-SWAP,PEOPLE-USDT-SWAP"
    url_3 = f"https://www.okx.com/priapi/v5/public/products?t={ts}&instType=SWAP&instId=PERP-USDT-SWAP,QTUM-USDT-SWAP,RACA-USDT-SWAP,RDNT-USDT-SWAP,REN-USDT-SWAP,RNDR-USDT-SWAP,RSR-USDT-SWAP,RVN-USDT-SWAP,SAND-USDT-SWAP,SHIB-USDT-SWAP,SLP-USDT-SWAP,SNX-USDT-SWAP,STORJ-USDT-SWAP,STX-USDT-SWAP,SUI-USDT-SWAP,SUSHI-USDT-SWAP,SWEAT-USDT-SWAP,THETA-USDT-SWAP,TON-USDT-SWAP,TRX-USDT-SWAP,UMA-USDT-SWAP,UNI-USDT-SWAP,USDC-USDT-SWAP,USTC-USDT-SWAP,VRA-USDT-SWAP,WAVES-USDT-SWAP,WAXP-USDT-SWAP,WLD-USDT-SWAP,WOO-USDT-SWAP,WSM-USDT-SWAP,XCH-USDT-SWAP,XLM-USDT-SWAP,XMR-USDT-SWAP,XTZ-USDT-SWAP,YFI-USDT-SWAP,YFII-USDT-SWAP,YGG-USDT-SWAP,ZEC-USDT-SWAP,ZEN-USDT-SWAP,ZIL-USDT-SWAP,ZRX-USDT-SWAP,BTC-USDC-SWAP,ETH-USDC-SWAP,BTC-USD-SWAP,ETH-USD-SWAP,LTC-USD-SWAP,XRP-USD-SWAP,BCH-USD-SWAP,SOL-USD-SWAP,FIL-USD-SWAP"

    url_lists = [url_1,url_2,url_3]
    symbol_faceValue = pd.DataFrame()

    for url in url_lists:
        ok_symbol_list = requests.get(url).json()['data']
        ok_symbol_FV = pd.DataFrame([{'symbol':i.get('instFamily').replace('-',''),'face_value':i.get('ctVal')} for i in ok_symbol_list])
        ok_symbol_FV['face_value'] = ok_symbol_FV['face_value'].astype(float)
        symbol_faceValue = pd.concat([symbol_faceValue,ok_symbol_FV],ignore_index=True)
        time.sleep(0.5)
    symbol_faceValue
    first_merge = pd.merge(ok_mp,symbol_faceValue,on='symbol',how='right')
    first_merge['value'] = first_merge['markPx']*first_merge['face_value']
    second_merge = pd.merge(first_merge,filter_okx,left_on='symbol',right_on='instFamily',how='right')
    # second_merge
    # filter_okx['maxSz'] = second_merge['value']*second_merge['maxSz']
    # filter_okx
    second_merge['maxSz'] = second_merge['maxSz'].astype(float)
    second_merge['value']*second_merge['maxSz']
    filter_okx['maxSz'] = second_merge['value']*second_merge['maxSz']
    print("OKX Download Completed!!! ")

    print('Start downloading Binance data')
    api_key = 'X7alDodZ6bQANZLutPROnUbbdPPYohaMJhpXxUV6YqMBtcrTC45QqCPPRg3LCkhW'
    api_secret = 'vGozshXG9ADwTrT1NDUEZr9EFP3ErMVQnCeCOBPhaQYqvOqrKtbMn8JnsaUzui2I'
    headers = {
    'Content-Type': 'application/json',
    'X-MBX-APIKEY': api_key}
    def get_ts():
        now = int(time.time()*1000)
        return str(now)
    def hashing(query_string):
        return hmac.new(
            api_secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256
        ).hexdigest()
    t = get_ts()
    url = "https://fapi.binance.com/fapi/v1/leverageBracket?timestamp="+str(t)+"&signature="+hashing("timestamp="+str(t))
    bracket = requests.get(url,headers=headers).json()

    bn_bracket = pd.DataFrame()
    for i in bracket:
        temp = pd.DataFrame(i['brackets'])
        temp.insert(0,'symbol',i['symbol'])
        bn_bracket = pd.concat([bn_bracket,temp],ignore_index=True)
        
    bn_funding = pd.read_json('funding.json')
    bn_funding[['symbol','funding_interval','current_cap_floor','default_cap_floor']]

    bn_funding['adj_cap_floor'] = bn_funding['current_cap_floor'].str.split(' / ',expand=True)[0].str.replace('%','').astype(float)
    bn_funding['default_cap_floor'] = bn_funding['default_cap_floor'].str.split(' / ',expand=True)[0].str.replace('%','').astype(float)
    bn_funding = bn_funding[['symbol','funding_interval','adj_cap_floor','default_cap_floor']]
    bn_funding['symbol'] = bn_funding['symbol'].str.replace(' Perpetual','')
    def check_adj(bn_funding):
        if bn_funding['adj_cap_floor'] == bn_funding['default_cap_floor'] and bn_funding['funding_interval'] == 8:
            return True
        else:
            return False
    unadjusted = bn_funding[bn_funding.apply(check_adj,axis=1)]
    adjusted_funding_url = "https://fapi.binance.com/fapi/v1/fundingInfo"
    adjusted = pd.DataFrame(requests.get(adjusted_funding_url).json()).iloc[:,[0,3,1,2]]
    adjusted['adjustedFundingRateCap'] = adjusted['adjustedFundingRateCap'].astype(float)
    adjusted['adjustedFundingRateFloor'] = adjusted['adjustedFundingRateFloor'].astype(float)
    adjusted.columns = unadjusted.columns 
    bn_funding = pd.concat([unadjusted,adjusted],ignore_index=True)
    bn_funding['adj_cap_floor'] = bn_funding['adj_cap_floor']*100
    bn_bracket = pd.merge(left=bn_bracket,right=bn_funding,on='symbol',how='left').dropna(subset=['funding_interval'])
    bn_bracket['exchange'] ='binance'
    bn_bracket.insert(6,'initialMargin',bn_bracket['initialLeverage'].astype(float).apply(lambda x: 1/x))
    print("Binance data download completed!!!")

    # 进行合并
    # okx: filter_okx
    # bybit: bybit_info
    # binance: bn_bracket
    # 统一列字段名
    # symbol,imr,mmr,amount_cap,amt_floor,leverage,funding_interval,exchange
    print("Start merge data")
    columns_name = ['symbol','imr','mmr','amount_cap','leverage','funding_cap','funding_interval','exchange']
    bn = bn_bracket[['symbol','initialMargin','maintMarginRatio','notionalCap','initialLeverage','adj_cap_floor','funding_interval','exchange']]
    ok = filter_okx[['instFamily','imr','mmr','maxSz','maxLever','fundingCap/Floor','funding_interval','exchange']]
    bybit = bybit_info[['symbol','initialMargin','maintainMargin','limit','maxLeverage','fundingRateClamp','fundingRateInterval','exchange']]

    bn.columns = columns_name
    ok.columns = columns_name
    ok[['imr','mmr','amount_cap','leverage','funding_cap','funding_interval']] = ok[['imr','mmr','amount_cap','leverage','funding_cap','funding_interval']].astype(float)
    bybit.columns = columns_name
    bybit[['funding_cap','funding_interval']] = bybit[['funding_cap','funding_interval']].astype(float)

    exchanges_brackets = pd.concat([bn,ok,bybit],ignore_index=True)
    exchanges_brackets['funding_interval'] = exchanges_brackets['funding_interval'].astype(int)

    print("Data merge completed!!!")
    return exchanges_brackets

if __name__ == '__main__':
    main()


