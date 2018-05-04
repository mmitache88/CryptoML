# -*- coding: utf-8 -*-
"""
Created on Fri Jan 12 17:12:48 2018

@author: mmita
"""
#%%
import sqlite3
import time
import datetime
import pandas as pd
import numpy as np
import os.path
#%%
LatestDf = pd.read_csv('D:\\Google Drive\\1 Desktop\\Projects\\Cryptos\\Datasets\\price related\\master_hourly_2018-04-27.csv') # Link to latest dataframe
                        
LatestDf = LatestDf.dropna(axis = 0, how='all') 
#%
cwd = os.getcwd()
#cwd = "C:\\Users\\mmita\\Desktop\\temp"
db_path = os.path.join (cwd, "HistoricPriceHourly.db")
conn= sqlite3.connect(db_path)
c= conn.cursor()
#%
def read_from_db():
    c.execute('SELECT Close, High, Low, Open, VolumeFrom, VolumeTo, Timestamp, Symbol, ComparisonSymbol FROM CryptoDataHourly')
    MasterCompareDf = pd.DataFrame(c.fetchall())
    MasterCompareDf.columns = ['Close', 'High', 'Low', 'Open', 'VolumeFrom', 'VolumeTo', 'Timestamp', 'Symbol', 'ComparisonSymbol']
    read_from_db.max_timestamp =  max(MasterCompareDf['Timestamp'])
    print(read_from_db.max_timestamp)
    
#%
read_from_db()  #Check for latest dataframe
UpdateDf =  LatestDf[LatestDf['timestamp']>read_from_db.max_timestamp]
UpdateDf = UpdateDf.reset_index(drop=True)
        
#%%
def create_table():
    c.execute('CREATE TABLE IF NOT EXISTS CryptoDataHourly(Close NUMERIC, High NUMERIC, Low NUMERIC, Open NUMERIC,VolumeFrom NUMERIC,VolumeTo NUMERIC, Timestamp TEXT, Symbol TEXT, ComparisonSymbol TEXT)')
    
#%%    
def dynamic_data_entry():
    c = conn.cursor()
    #Close = [(i,) for i in list(MasterDf['close'])]
    Close = UpdateDf['close']
    High = UpdateDf['high']
    Low = UpdateDf['low']
    Open = UpdateDf['open']
    #Time = UpdateDf['time']
    VolumeFrom = UpdateDf['volumefrom']
    VolumeTo = UpdateDf['volumeto']
    Timestamp = UpdateDf['timestamp']
    Symbol = UpdateDf['Symbol']
    ComparisonSymbol = UpdateDf['Comparison_symbol']
    for i in range(0, len(Symbol)):
        c.execute("INSERT INTO CryptoDataHourly(Close, High, Low, Open, VolumeFrom, VolumeTo, Timestamp, Symbol, ComparisonSymbol) VALUES (?,?,?,?,?,?,?,?,?)", (Close[i],High[i],Low[i],Open[i],VolumeFrom[i],VolumeTo[i],Timestamp[i],Symbol[i],ComparisonSymbol[i]))
    conn.commit()
    c.close()
    conn.close()
    
#%%
create_table()

dynamic_data_entry()


     
    

