# -*- coding: utf-8 -*-
"""
Created on Thu Jan  4 23:02:00 2018

@author: mmita
"""

#%%
import requests
import datetime
import pandas as pd
import matplotlib.pyplot as plt
#%%
def price(symbol, comparison_symbols=['USD'], exchange=''):
    url = 'https://min-api.cryptocompare.com/data/price?fsym={}&tsyms={}'\
            .format(symbol.upper(), ','.join(comparison_symbols).upper())
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()
    return data

#%%
price('LTC', exchange='Coinbase')
#%%
def daily_price_historical(symbol, comparison_symbol, all_data=True, limit=1, aggregate=1, exchange=''):
    url = 'https://min-api.cryptocompare.com/data/histoday?fsym={}&tsym={}&limit={}&aggregate={}'\
            .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate)
    if exchange:
        url += '&e={}'.format(exchange)
    if all_data:
        url += '&allData=true'
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    df.insert(8,'Symbol',symbol)
    df.insert(9,'Comparison_symbol',comparison_symbol)
    return df
#%%
df = daily_price_historical('BTC', 'USD')
#%%
def hourly_price_historical(symbol, comparison_symbol, limit, aggregate, exchange=''):
    url = 'https://min-api.cryptocompare.com/data/histohour?fsym={}&tsym={}&limit={}&aggregate={}'\
            .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    df.insert(8,'Symbol',symbol)
    df.insert(9,'Comparison_symbol',comparison_symbol)
    return df
#%%
#%%
def minute_price_historical(symbol, comparison_symbol, limit, aggregate, exchange=''):
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={}&limit={}&aggregate={}'\
            .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    df.insert(8,'Symbol',symbol)
    df.insert(9,'Comparison_symbol',comparison_symbol)
    return df
#%%
time_delta = 1
df_hourly = hourly_price_historical('BTC','BTC',9999, time_delta)

#df_hourly_PIZZA

#%%
tickers = ['BTC','XRP','ETH','XLM','ADA','LTC','NEO','XMR','XVG','IOT','EOS',
'BCH','DASH','QTUM','DOGE','ETC','ZEC','LSK','XRB','SNT','SC','GAS','GNT','XZC']
comp = ['USD','BTC']
time_delta = 1
#%%
df = []
for i in range(0,len(tickers)):
    for j in range(0, len(comp)):
        try:
            df.append(hourly_price_historical(tickers[i],comp[j],9999,time_delta))
        except AttributeError:
            print('Error')
#%%    
df_master = pd.concat(df, axis=0)
#%%
date = datetime.date.today()
#%%    
df_master.to_csv('D:\\Google Drive\\1 Desktop\\Projects\\Cryptos\\Datasets\\price related\\master_hourly_%s.csv' %(date))
