# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 13:25:18 2018

@author: mitachem
"""
import sqlite3
import time
import datetime
import pandas as pd
import numpy as np
import os.path
import matplotlib.pyplot as plt
from matplotlib.finance import candlestick2_ochl
#%%
path = "D:\\Google Drive\\1 Desktop\\Projects\\Cryptos\\Datasets\\price related\\sqlite"
#path = "C:\\Users\\mitachem\\Desktop\\Personal\\Projects\\Cryptos\\Datasets\\price related\\sqlite"

dbName = "HistoricPriceHourly.db"
tableName = "CryptoDataHourlyEnriched"

#Read in latest hourly data
from loaddata import Load_Data_Class
init_Load_Data = Load_Data_Class(path,dbName)
MasterDf = Load_Data_Class(path,dbName).read_from_db(tableName)
#%% Some additional rolling sums over the Closing price percentages
MasterDf['Labels'] = 0   
MasterDf['RollingClosePercentage6'] = MasterDf['ClosePercentageChange'].rolling(6).sum()
MasterDf['RollingClosePercentage12'] = MasterDf['ClosePercentageChange'].rolling(12).sum()
MasterDf['RollingClosePercentage18'] = MasterDf['ClosePercentageChange'].rolling(18).sum()
MasterDf['RollingClosePercentage24'] = MasterDf['ClosePercentageChange'].rolling(24).sum()
#%% Generating labels
#MasterDf_ref = MasterDf[['Timestamp','Symbol','ComparisonSymbol','Close','ClosePercentageChange','RollingClosePercentage6','RollingClosePercentage12','RollingClosePercentage18','RollingClosePercentage24','Trend6h','Trend0h']]
ind1 = MasterDf[MasterDf['RollingClosePercentage6']>=5].index.values # red - sell CONSIDER changing this to Close price instead of Rolling Close
#ind1 = MasterDf_ref[MasterDf_ref['ClosePercentageChange']>=3].index.values # red - sell CONSIDER changing this to Close price instead of Rolling Close
ind2 = MasterDf[MasterDf['RollingClosePercentage6']<=-5].index.values # buy - blue
MasterDf.loc[ind1,'Labels'] = 1 #sell
MasterDf.loc[ind2,'Labels'] = 2 #buy
MasterDf = MasterDf.drop(['RollingClosePercentage6','RollingClosePercentage12','RollingClosePercentage18','RollingClosePercentage24'], axis=1)

#%% Saving Labelled dataset    
path = "D:\\Google Drive\\1 Desktop\\Projects\\Cryptos\\Datasets\\price related\\sqlite"
#path = "C:\\Users\\mitachem\\Desktop\\Personal\\Projects\\Cryptos\\Datasets\\price related\\sqlite"
dbName = "HistoricPriceHourly.db"
tableName = "CryptoDataHourlyEnrichedLabelled"

from savedata import SaveDataClass
SaveDataClass = SaveDataClass(path,dbName,tableName)
SaveDataClass.create_table()
SaveDataClass.dynamic_data_entry(MasterDf)

#%%

from profitcalculation import ProfitCalc
symbol = 'BTC'
ProfitCalc = ProfitCalc(MasterDf, symbol)
ProfitCalc.calcEarnings()

#%% Plotting Figure 1
MasterDf['Timestamp'] = pd.to_datetime(MasterDf['Timestamp'])
symbol_one = pd.unique(MasterDf['Symbol'])
symbol_two = pd.unique(MasterDf['ComparisonSymbol'])

dfmasterfinal_temp = MasterDf[(MasterDf['Symbol']==symbol_one[0]) & (MasterDf['ComparisonSymbol']==symbol_two[0])] #& (MasterDf['Timestamp']>'2018-01-01')]
plt.figure(1, figsize=(20,10))
plt.subplot(2,1,1)
plt.plot(dfmasterfinal_temp['Timestamp'],dfmasterfinal_temp['Close'], color='k')
plt.plot(dfmasterfinal_temp['Timestamp'],dfmasterfinal_temp['MovingAverage12h'], color='g')
plt.plot(dfmasterfinal_temp['Timestamp'],dfmasterfinal_temp['ExpMovingAverage12h'], color='r')
#plt.plot(dfmasterfinal_temp['timestamp'],b[2], color='b')
plt.subplot(2,1,2)
plt.plot(dfmasterfinal_temp['Timestamp'],dfmasterfinal_temp['RelativeStrengthIndex'], color='black')
plt.axhline(y=30, color='b')
plt.axhline(y=70, color='r')
#master_hour_temp['MovingAverage3h'] = movingaverage(master_hour_temp['close'],5)
#%% Plotting 2
MasterDf['Timestamp'] = pd.to_datetime(MasterDf['Timestamp'])


dfmasterfinal_temp = MasterDf[(MasterDf['Symbol']==symbol_one[0]) & (MasterDf['ComparisonSymbol']==symbol_two[0]) & (MasterDf['Timestamp']>'2018-01-01')]
plt.figure(2, figsize=(20,10))

plt.subplot(2,1,1)
plt.plot(dfmasterfinal_temp['Timestamp'],dfmasterfinal_temp['Close'], color='k')
plt.plot(dfmasterfinal_temp['Timestamp'],dfmasterfinal_temp['Open'], color='k')
#plt.plot(dfmasterfinal_temp['timestamp'],b[2], color='b')
plt.subplot(2,1,2)
plt.plot(dfmasterfinal_temp['Timestamp'],dfmasterfinal_temp['ClosePercentageChange'], color='black')
plt.axhline(y= 2, color='b')
plt.axhline(y= 0, color='black')
plt.axhline(y= -2, color='r')
#master_hour_temp['MovingAverageh'] = movingaverage(master_hour_temp['close'],5)
#%% Plotting 3
             
ind1 = MasterDf[MasterDf['Labels']==1].index.values # red - sell CONSIDER changing this to Close price instead of Rolling Close
ind2 = MasterDf[MasterDf['Labels']==2].index.values # buy - blue
                
MasterDf['Timestamp'] = pd.to_datetime(MasterDf['Timestamp'])
symbol_one = pd.unique(MasterDf['Symbol'])
symbol_two = pd.unique(MasterDf['ComparisonSymbol'])
dfmasterfinal_temp = MasterDf[(MasterDf['Symbol']==symbol_one[0]) & (MasterDf['ComparisonSymbol']==symbol_two[0])]# & (MasterDf_ref['Timestamp']>'2018-01-01')] #& (MasterDf['Timestamp']>'2018-01-01')]

plt.figure(1, figsize=(20,10))
plt.subplot(2,1,1)
plt.plot(dfmasterfinal_temp['Timestamp'],dfmasterfinal_temp['Close'], color='k')
plt.plot_date(dfmasterfinal_temp['Timestamp'][ind1],dfmasterfinal_temp['Close'][ind1], marker='+', color='red') # sell
plt.plot_date(dfmasterfinal_temp['Timestamp'][ind2],dfmasterfinal_temp['Close'][ind2], marker='+', color='blue') # buy
#plt.plot(dfmasterfinal_temp['timestamp'],b[2], color='b')
plt.subplot(2,1,2)
#plt.plot(dfmasterfinal_temp['Timestamp'],dfmasterfinal_temp['Trend6h'], color='black')
plt.plot(dfmasterfinal_temp['Timestamp'],dfmasterfinal_temp['Labels'], color='blue')
plt.axhline(y=0, color='b')
plt.axhline(y=1.5, color='r')
