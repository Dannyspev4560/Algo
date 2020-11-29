import alpaca_trade_api as tradeapi
import bot.alpaca_key as keys
import time
import numpy as np
import pandas as pd
import datetime as dt
import yfinance as yf
import csv
import asyncio


def ATR(DF,n):
    "function to calculate True Range and Average True Range"
    df = DF.copy()
    df['H-L']=abs(df['High']-df['Low'])
    df['H-PC']=abs(df['High']-df['Adj Close'].shift(1))
    df['L-PC']=abs(df['Low']-df['Adj Close'].shift(1))
    df['TR']=df[['H-L','H-PC','L-PC']].max(axis=1,skipna=False)
    df['ATR'] = df['TR'].rolling(n).mean()
    #df['ATR'] = df['TR'].ewm(span=n,adjust=False,min_periods=n).mean()
    df2 = df.drop(['H-L','H-PC','L-PC'],axis=1)
    return df2['ATR']

def TR(DF):
    df = DF.copy()
    df['H-L'] = abs(df['High'] - df['Low'])
    df['H-PC'] = abs(df['High'] - df['Adj Close'].shift(1))
    df['L-PC'] = abs(df['Low'] - df['Adj Close'].shift(1))
    df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1, skipna=False)
    return df['TR']


def get_tickers():
    #url = 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt' # all nasdaq tickers
    #df = pd.read_csv(url, sep='|')
    #tickers=df['Symbol'].tolist()
    tickers = []
    good_tickers={}
    with open('C:\\Users\\danny\\coding\\algo\\udemyCourse1\\bot\\companylist_nasdaq.csv', 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if row[0]!='Symbol' and row[5]=='Technology':#technology sector
                tickers.append(row[0])
    start = dt.datetime.today() - dt.timedelta(28)
    end=dt.datetime.today()
    for ticker in tickers:
        try:
            df=yf.download(ticker, start, end)
            close_price=df["Adj Close"].tail(1).iloc[0]
            ATR_last=ATR(df,14).tail(1).iloc[0]
            TR_last=TR(df).tail(1).iloc[0]
            if (close_price>10 and close_price<100 and df["Volume"].mean()>1000000):
                if (ATR_last> 0.75 and TR_last>ATR_last):
                    #add true range check- if today greater than ATR -means that we expecting high volatility
                    if(df["Adj Close"].pct_change().iloc[-1]>0):
                        good_tickers[ticker]=df["Adj Close"].pct_change().iloc[-1]

        except:
            print("bad ticker: {}".format(ticker))


    return good_tickers.keys()

class ticker:
    def __init__(self,name):
        self.name=name
        self.signal=""
        self.DF=pd.DataFrame
    def name(self):
        return self.name()
    def set_signal(self,sig):
       self.signal=sig
    def get_signal(self):
        return self.signal
    def set_DF(self,df):
        self.DF=df
    def get_DF(self):
        return self.DF
    def sell(self,api):
        api.submit_order(
            symbol=self.name,
            qty=1,
            side='sell',
            type='market',
            time_in_force='gtc',
            client_order_id=self.name
        )
    def buy(self,api):
        api.submit_order(
            symbol=self.name,
            qty=1,
            side='buy',
            type='market',
            time_in_force='gtc',
            client_order_id=self.name
        )


class BreakoutStrategy:
    def __init__(self,api,tickers):
        self._api = api
        self._tickers=tickers

    def tickers(self):
        return self._tickers
    def run(self):
        stocks_list = {}
        #downloading data for each
        for ticker in self._tickers:
            barset = self._api.get_barset(symbols=ticker.name,timeframe='1Min', limit=22)
            #aapl_bars = barset['AAPL']
            df=barset.df
            df.columns=["Open","High","Low","Adj Close","Volume"]
            df["ATR"] = ATR(df, 20)
            df["roll_max_cp"] = df["High"].rolling(20).max()
            df["roll_min_cp"] = df["Low"].rolling(20).min()
            df["roll_max_vol"] = df["Volume"].rolling(20).max()
            df.dropna(inplace=True)
            ticker.set_DF(df)
            stocks_list[ticker.name]=ticker
            #print(df)

            #strategy logic
            #signal=""
            if ticker.get_signal()=="":
                if df["High"].iloc[-1] >= df["roll_max_cp"].iloc[-1] and df["Volume"].iloc[-1] > 1.5 * df["roll_max_vol"].iloc[-2]:
                    #signal = "Buy"  # not the same signal
                    ticker.set_signal("long")
                    ticker.buy(self._api)
                elif df["Low"].iloc[-1] <= df["roll_min_cp"].iloc[-1] and df["Volume"].iloc[-1] > 1.5 * df["roll_max_vol"].iloc[-2]:
                    #signal = "Sell"
                    ticker.set_signal("short")
                    ticker.sell(self._api)

            if ticker.get_signal()=="long":
                if df["Adj Close"].iloc[-1] < df["Adj Close"].iloc[-2] - df["ATR"].iloc[-2]:
                    #signal = ""
                    ticker.set_signal("")
                elif df["Low"].iloc[-1] <= df["roll_min_cp"].iloc[-1] and \
                        df["Volume"].iloc[-1] > 1.5 * df["roll_max_vol"].iloc[-2]:
                    #signal = "Sell"
                    ticker.set_signal("")
                    ticker.sell(self._api)

            if ticker.get_signal()=="short":
                if df["Adj Close"].iloc[-1] > df["Adj Close"].iloc[-2] + df["ATR"].iloc[-2]:
                    signal = ""
                elif df["High"].iloc[-1] >= df["roll_max_cp"].iloc[-1] and df["Volume"].iloc[-1] > 1.5 * df["roll_max_vol"].iloc[-2]:
                    signal = "Buy"
                    ticker.set_signal("")
                    ticker.buy(self._api)

api = tradeapi.REST(keys.API_Key, keys.Secret_Key, base_url='https://paper-api.alpaca.markets')
clock = api.get_clock()
while(1):
    starttime = time.time()
    get_tickers()
    symbols=[]
    bot=""
    todays_open_time=clock.next_open
    if not clock.is_open:
        if (clock.next_close - clock.timestamp).total_seconds() < 900:#15 min window to close
            api.close_all_positions()# close all postions and gather data(daily retrun)

        if (clock.timestamp -todays_open_time).total_seconds() < 1800:
            if len(symbols) < 5:
                try:
                    symbols = get_tickers()
                except:
                    print('Error retrieving data ')
                    #send me email and add other alternatives- stmp optional
                    break
            time.sleep(1800)#30 min sleep till i have more data about the stocks
        elif bot=="":
            tickers = []
            for symbol in symbols:
                #print(symbol)
                tickers.append(ticker(symbol))
            bot= BreakoutStrategy(api, tickers)
            bot.run()
            time.sleep(60)
            #time.sleep(time.time()-starttime +1)# +1 Safety factor
        else:
            bot.run()
            time.sleep(60)

    else:
        if len(symbols)<5:
            for i in range(0, 10):
                while True:
                    try:
                        symbols = get_tickers()
                    except:
                        time.sleep(300)#5 mins between each try
                        continue
                    break
        todays_open_time=clock.next_open
        time_to_open = clock.next_open - clock.timestamp
        print("time till market opens: {}".format(time_to_open))
        time.sleep(600)




"""
async def barseCall(api,tickers):
    stocks = {}
    for tick in tickers:
        barset = await api.get_barset(symbols=tick, timeframe='1Min', limit=22)
        df = barset.df
        df.columns = ["Open", "High", "Low", "Adj Close", "Volume"]
        stocks["tick"]=df
    return stocks

"""
"""
starttime=time.time()
stocks = {}

for tick in symbols:
    async barset = api.get_barset(symbols=tick, timeframe='1Min', limit=22)

print(time.time()-starttime)
"""