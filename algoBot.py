import alpaca_trade_api as tradeapi
import bot.alpaca_key as keys
import time
import pandas as pd
import yfinance as yf
import csv
import datetime as dt

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
    if(len(good_tickers)>6):
        sorted_dict = dict(sorted(good_tickers.items(), key=lambda item: item[1], reverse=True)[:6])# get top 6()
        return sorted_dict.keys()

    else: return good_tickers.keys()

class ticker:
    def __init__(self,name):
        self.name=name
        self.signal=""
        self.DF=pd.DataFrame
        self.sample_time=0
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
    def set_sample_time(self,time):
        self.sample_time=time
    def sample_time(self):
        return self.sample_time
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
            barset = self._api.get_barset(symbols=ticker.name,timeframe='1Min', limit=18)
            df=barset.df
            # due to demo issue-cant get live data of every minute-if the last sample time hasnt changed we wont enter this iiteration
            dummy=df.copy()
            dummy.reset_index(inplace=True)
            if not ticker.sample_time:
                ticker.set_sample_time(dummy["index"].iloc[-1])
            elif(ticker.sample_time!= dummy["index"].iloc[-1]):
                ticker.set_sample_time(df.keys[::-1])
            else: break #exit this iiteration
            df.columns=["Open","High","Low","Adj Close","Volume"]
            df["ATR"] = ATR(df, 14)
            df["roll_max_cp"] = df["High"].rolling(14).max()
            df["roll_min_cp"] = df["Low"].rolling(14).min()
            df["roll_max_vol"] = df["Volume"].rolling(14).max()
            df.dropna(inplace=True)
            ticker.set_DF(df)
            stocks_list[ticker.name]=ticker

            #strategy logic


            if ticker.get_signal()=="":
                if df["High"].iloc[-1] >= df["roll_max_cp"].iloc[-1] and df["Volume"].iloc[-1] > 1.5 * df["roll_max_vol"].iloc[-2]:
                    ticker.set_signal("buy")#long
                    ticker.buy(self._api)
                elif df["Low"].iloc[-1] <= df["roll_min_cp"].iloc[-1] and df["Volume"].iloc[-1] > 1.5 * df["roll_max_vol"].iloc[-2]:
                    ticker.set_signal("sell")#short
                    ticker.sell(self._api)

            if ticker.get_signal()=="buy":
                if df["Adj Close"].iloc[-1] < df["Adj Close"].iloc[-2] - df["ATR"].iloc[-2]:
                    ticker.set_signal("")#0 shares
                    ticker.sell(self._api)
                elif df["Low"].iloc[-1] <= df["roll_min_cp"].iloc[-1] and \
                        df["Volume"].iloc[-1] > 1.5 * df["roll_max_vol"].iloc[-2]:
                    ticker.set_signal("sell")
                    ticker.sell(self._api)#0 shares
                    ticker.sell(self._api)#enter short

            if ticker.get_signal()=="sell":
                if df["Adj Close"].iloc[-1] > df["Adj Close"].iloc[-2] + df["ATR"].iloc[-2]:
                    ticker.set_signal("")
                    ticker.buy(self._api)#0 shares
                elif df["High"].iloc[-1] >= df["roll_max_cp"].iloc[-1] and df["Volume"].iloc[-1] > 1.5 * df["roll_max_vol"].iloc[-2]:
                    ticker.set_signal("buy")
                    ticker.buy(self._api)#0 shares
                    ticker.buy(self._api)#enter long



api = tradeapi.REST(keys.API_Key, keys.Secret_Key, base_url='https://paper-api.alpaca.markets')
bot=None
todays_open_time=api.get_clock().next_open #must be deployed at noon every day
symbols=[]
while(1):
    starttime = time.time()
    #get_tickers()
    attempts=3
    if not(symbols):
        while(attempts ):
            try:
                symbols=get_tickers()
                break
            except:
                print('Error retrieving data...retrying  ')
                symbols = get_tickers()
                attempts-=1

    if api.get_clock().is_open and symbols:
        if (api.get_clock().next_close - api.get_clock().timestamp).total_seconds() < 900:#15 min window to close
            api.close_all_positions()# close all postions
            print("exiting")
            break
        if (api.get_clock().timestamp -todays_open_time).total_seconds() < 1200:

            time.sleep(1200-api.get_clock().timestamp -todays_open_time)#20 min sleep till i have more data about the stocks
        elif not bot:
            tickers = []
            for symbol in symbols:
                #print(symbol)
                tickers.append(ticker(symbol))
            bot= BreakoutStrategy(api, tickers)
            try:
                #time.sleep(60-api.get_clock().timestamp.to_pydatetime().second)
                bot.run()
                #time.sleep(60)
                time.sleep(60-api.get_clock().timestamp.to_pydatetime().second)#set script clock as close asvpossible to api's clock
            except:
                #time.sleep(60)
                time.sleep(60-api.get_clock().timestamp.to_pydatetime().second)
        else:
            try:
                bot.run()
                time.sleep(60)
                #  time.sleep(60-api.get_clock().timestamp.to_pydatetime().second)
            except:
                time.sleep(60)
                #  time.sleep(60-api.get_clock().timestamp.to_pydatetime().second)
    elif not symbols:
        #notify by mail-bo tickers
        break
    else:

        todays_open_time=api.get_clock().next_open
        time_to_open = api.get_clock().next_open - api.get_clock().timestamp
        print("Timestamp:{} Time till market opens: {}".format(dt.datetime.now(),time_to_open))
        time.sleep(time_to_open.seconds)
