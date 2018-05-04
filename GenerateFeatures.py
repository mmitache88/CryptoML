# -*- coding: utf-8 -*-
"""
Created on Mon Jan 22 19:34:14 2018

@author: mmita
Load Data
This script generates features
"""
import sqlite3
import pandas as pd
import os.path
import time

path = "D:\\Google Drive\\1 Desktop\\Projects\\Cryptos\\Datasets\\price related\\sqlite"
#path = "C:\\Users\\mitachem\\Desktop\\Personal\\Projects\\Cryptos\\Datasets\\price related\\sqlite"

dbName = "HistoricPriceHourly.db"
tableName = "CryptoDataHourly"
#Read in latest hourly data
from loaddata import Load_Data_Class
init_Load_Data = Load_Data_Class(path,dbName)
MasterDf = init_Load_Data.read_from_db(tableName)

#%For loop iterating through master dataframe
from featurefunctions import GenerateFeaturesClass 
GF = GenerateFeaturesClass()

smawindow=12
ListMasterFinal = []
symbol_one = pd.unique(MasterDf['Symbol'])
symbol_two = pd.unique(MasterDf['ComparisonSymbol'])
start = time.time()
print("start timer")
for i in range(0,len(symbol_one)):
    for j in range(0,len(symbol_two)):
        MasterDf_temp = MasterDf[(MasterDf['Symbol']==symbol_one[i]) & (MasterDf['ComparisonSymbol']==symbol_two[j])] 
        MasterDf_temp = MasterDf_temp.reset_index(drop=True)
        
        len_df=len(MasterDf_temp['Timestamp'])
        MasterDf_temp.loc[0:len_df,'Timestamp']= pd.to_datetime(MasterDf_temp.loc[0:len_df,'Timestamp'])
        MasterDf_temp.sort_values(by=['Timestamp'])
        
        MasterDf_temp['HighLowRatio'] = 0
        MasterDf_temp['OpenCloseRatio'] = 0
        MasterDf_temp['MovingAverage'+ str(smawindow) +'h'] = 0 
        MasterDf_temp['ExpMovingAverage'+ str(smawindow) +'h'] = 0
        MasterDf_temp['RelativeStrengthIndex'] = 0
        MasterDf_temp['EMAfast'] = 0
        MasterDf_temp['EMAslow'] = 0
        
        MasterDf_temp['Close' + 'PercentageChange'] = 0
        MasterDf_temp['High' + 'PercentageChange'] = 0
        MasterDf_temp['Low' + 'PercentageChange'] = 0
        MasterDf_temp['Open' + 'PercentageChange'] = 0
        MasterDf_temp['VolumeFrom' + 'PercentageChange'] = 0
        MasterDf_temp['VolumeTo' + 'PercentageChange'] = 0
        MasterDf_temp['HighLowRatio' + 'PercentageChange'] = 0
        MasterDf_temp['OpenCloseRatio' + 'PercentageChange'] = 0
        MasterDf_temp['MovingAverage12h' + 'PercentageChange'] = 0
        MasterDf_temp['ExpMovingAverage12h' + 'PercentageChange'] = 0
        MasterDf_temp['RelativeStrengthInd' + 'PercentageChange'] = 0
        MasterDf_temp['EMAfast' + 'PercentageChange'] = 0
        MasterDf_temp['EMAslow' + 'PercentageChange'] = 0
        
        
        if len(MasterDf_temp['Timestamp']) > smawindow:
            
            MasterDf_temp.loc[0:len_df,('HighLowRatio')] = MasterDf_temp.loc[0:len_df,('High')] / MasterDf_temp.loc[0:len_df,('Low')]
            MasterDf_temp.loc[0:len_df,('OpenCloseRatio')] = MasterDf_temp.loc[0:len_df,('Open')] / MasterDf_temp.loc[0:len_df,('Close')]

            MasterDf_temp.loc[smawindow-1:len_df,('MovingAverage'+ str(smawindow) +'h')] = GF.movingaverage(MasterDf_temp['Close'], smawindow)
            
            MasterDf_temp.loc[0:len_df,('ExpMovingAverage'+ str(smawindow) +'h')] = GF.ExpMovingAverage(MasterDf_temp['Close'], smawindow)
            
            MasterDf_temp.loc[0:len_df,('RelativeStrengthIndex')] = GF.rsiFunc(MasterDf_temp['Close'])
            
            MasterDf_temp.loc[0:len_df,('EMAslow')] = GF.computeMACD(MasterDf_temp['Close'])[0]
            
            MasterDf_temp.loc[0:len_df,('EMAfast')] = GF.computeMACD(MasterDf_temp['Close'])[1]
            
            MasterDf_temp.loc[0:len_df,('ClosePercentageChange')] = GF.percentageDif(MasterDf_temp['Close']).shift(1)
            MasterDf_temp.loc[0:len_df,('HighPercentageChange')] = GF.percentageDif(MasterDf_temp['High']).shift(1)
            MasterDf_temp.loc[0:len_df,('LowPercentageChange')] = GF.percentageDif(MasterDf_temp['Low']).shift(1)
            MasterDf_temp.loc[0:len_df,('OpenPercentageChange')] = GF.percentageDif(MasterDf_temp['Open']).shift(1)
            MasterDf_temp.loc[0:len_df,('VolumeFromPercentageChange')] = GF.percentageDif(MasterDf_temp['VolumeFrom']).shift(1)
            MasterDf_temp.loc[0:len_df,('VolumeToPercentageChange')] = GF.percentageDif(MasterDf_temp['VolumeTo']).shift(1)
            MasterDf_temp.loc[0:len_df,('HighLowRatioPercentageChange')] = GF.percentageDif(MasterDf_temp['HighLowRatio']).shift(1)
            MasterDf_temp.loc[0:len_df,('OpenCloseRatioPercentageChange')] = GF.percentageDif(MasterDf_temp['OpenCloseRatio']).shift(1)
            MasterDf_temp.loc[0:len_df,('MovingAverage12hPercentageChange')] = GF.percentageDif(MasterDf_temp['MovingAverage12h']).shift(1)
            MasterDf_temp.loc[0:len_df,('ExpMovingAverage12hPercentageChange')] = GF.percentageDif(MasterDf_temp['ExpMovingAverage12h']).shift(1)
            MasterDf_temp.loc[0:len_df,('RelativeStrengthIndPercentageChange')] = GF.percentageDif(MasterDf_temp['RelativeStrengthIndex']).shift(1)
            MasterDf_temp.loc[0:len_df,('EMAfastPercentageChange')] = GF.percentageDif(MasterDf_temp['EMAfast']).shift(1)
            MasterDf_temp.loc[0:len_df,('EMAslowPercentageChange')] = GF.percentageDif(MasterDf_temp['EMAslow']).shift(1)
        
        ListMasterFinal.append(MasterDf_temp)
            
        #print (symbol_one[i])
end = time.time()
print(end - start)

MasterFinalDf = pd.concat(ListMasterFinal, axis=0)    

# Split dataframes based on Comparison Symbol
CleanedMasterFinalDf= MasterFinalDf.dropna(axis=0, how='any')

CleanedMasterFinalDf.isnull().sum()

CleanedMasterFinalDf = CleanedMasterFinalDf.reset_index(drop=True)
#%%
#path = "C:\\Users\\mitachem\\Desktop\\Personal\\Projects\\Cryptos\\Datasets\\price related\\sqlite"
path = "D:\\Google Drive\\1 Desktop\\Projects\\Cryptos\\Datasets\\price related\\sqlite"

dbName = "HistoricPriceHourly.db"
#%%
from savedata import SaveDataClass
tablename = 'CryptoDataHourlyEnriched'
init_Save_Data = SaveDataClass(path,dbName, tablename)
init_Save_Data.create_table()
init_Save_Data.dynamic_data_entry(CleanedMasterFinalDf)
#SaveDataClass(path,dbName).create_table(tablename)
#SaveDataClass(path,dbName).dynamic_data_entry(tablename, CleanedMasterFinalDf)

