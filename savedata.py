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

class SaveDataClass:
    
    def __init__ (self, path, dbName, tablename):
        db_path = os.path.join (path, dbName)
        self.conn= sqlite3.connect(db_path)
        self.tablename = tablename
          
        
    def create_table(self):
        self.c = self.conn.cursor()
        if self.tablename == 'CryptoDataHourlyEnriched':
            self.c.execute('CREATE TABLE IF NOT EXISTS {} (Close NUMERIC, High NUMERIC, Low NUMERIC, Open NUMERIC, VolumeFrom NUMERIC, VolumeTo NUMERIC, Timestamp TEXT, Symbol TEXT, ComparisonSymbol TEXT, HighLowRatio NUMERIC, OpenCloseRatio NUMERIC, MovingAverage12h NUMERIC, ExpMovingAverage12h NUMERIC, RelativeStrengthIndex NUMERIC, EMAfast NUMERIC, EMAslow NUMERIC, ClosePercentageChange NUMERIC, HighPercentageChange NUMERIC, LowPercentageChange NUMERIC, OpenPercentageChange NUMERIC, VolumeFromPercentageChange NUMERIC, VolumeToPercentageChange NUMERIC, HighLowRatioPercentageChange NUMERIC, OpenCloseRatioPercentageChange NUMERIC, MovingAverage12hPercentageChange NUMERIC, ExpMovingAverage12hPercentageChange NUMERIC, RelativeStrengthIndPercentageChange NUMERIC, EMAfastPercentageChange NUMERIC, EMAslowPercentageChange NUMERIC)'.format(self.tablename))
        elif self.tablename == 'CryptoDataHourlyEnrichedLabelled':
            self.c.execute('CREATE TABLE IF NOT EXISTS {} (Close NUMERIC, High NUMERIC, Low NUMERIC, Open NUMERIC, VolumeFrom NUMERIC, VolumeTo NUMERIC, Timestamp TEXT, Symbol TEXT, ComparisonSymbol TEXT, HighLowRatio NUMERIC, OpenCloseRatio NUMERIC, MovingAverage12h NUMERIC, ExpMovingAverage12h NUMERIC, RelativeStrengthIndex NUMERIC, EMAfast NUMERIC, EMAslow NUMERIC, ClosePercentageChange NUMERIC, HighPercentageChange NUMERIC, LowPercentageChange NUMERIC, OpenPercentageChange NUMERIC, VolumeFromPercentageChange NUMERIC, VolumeToPercentageChange NUMERIC, HighLowRatioPercentageChange NUMERIC, OpenCloseRatioPercentageChange NUMERIC, MovingAverage12hPercentageChange NUMERIC, ExpMovingAverage12hPercentageChange NUMERIC, RelativeStrengthIndPercentageChange NUMERIC, EMAfastPercentageChange NUMERIC, EMAslowPercentageChange NUMERIC, Labels NUMERIC)'.format(self.tablename))
    
    def dynamic_data_entry(self, UpdateDf):
        self.c = self.conn.cursor()
        Close = UpdateDf['Close']
        High = UpdateDf['High']
        Low = UpdateDf['Low']
        Open = UpdateDf['Open']
        VolumeFrom = UpdateDf['VolumeFrom']
        VolumeTo = UpdateDf['VolumeTo']
        Timestamp = UpdateDf['Timestamp']
        Symbol = UpdateDf['Symbol']
        ComparisonSymbol = UpdateDf['ComparisonSymbol']
        HighLowRatio = UpdateDf['HighLowRatio']
        OpenCloseRatio = UpdateDf['OpenCloseRatio']
        MovingAverage12h = UpdateDf['MovingAverage12h'] 
        ExpMovingAverage12h = UpdateDf['ExpMovingAverage12h']
        RelativeStrengthIndex = UpdateDf['RelativeStrengthIndex']
        EMAfast = UpdateDf['EMAfast']
        EMAslow = UpdateDf['EMAslow']
        ClosePercentageChange = UpdateDf['ClosePercentageChange']
        HighPercentageChange = UpdateDf['HighPercentageChange']
        LowPercentageChange = UpdateDf['LowPercentageChange']
        OpenPercentageChange = UpdateDf['OpenPercentageChange']
        VolumeFromPercentageChange = UpdateDf['VolumeFromPercentageChange']
        VolumeToPercentageChange = UpdateDf['VolumeToPercentageChange']
        HighLowRatioPercentageChange = UpdateDf['HighLowRatioPercentageChange']
        OpenCloseRatioPercentageChange = UpdateDf['OpenCloseRatioPercentageChange']
        MovingAverage12hPercentageChange = UpdateDf['MovingAverage12hPercentageChange']
        ExpMovingAverage12hPercentageChange = UpdateDf['ExpMovingAverage12hPercentageChange']
        RelativeStrengthIndPercentageChange = UpdateDf['RelativeStrengthIndPercentageChange']
        EMAfastPercentageChange = UpdateDf['EMAfastPercentageChange']
        EMAslowPercentageChange = UpdateDf['EMAslowPercentageChange']
        if self.tablename == 'CryptoDataHourlyEnriched':
            for i in range(0, len(Symbol)):
                self.c.execute("INSERT INTO {} (Close, High, Low, Open, VolumeFrom, VolumeTo, Timestamp, Symbol, ComparisonSymbol, HighLowRatio, OpenCloseRatio, MovingAverage12h, ExpMovingAverage12h, RelativeStrengthIndex, EMAfast, EMAslow, ClosePercentageChange, HighPercentageChange, LowPercentageChange, OpenPercentageChange, VolumeFromPercentageChange, VolumeToPercentageChange, HighLowRatioPercentageChange, OpenCloseRatioPercentageChange, MovingAverage12hPercentageChange, ExpMovingAverage12hPercentageChange, RelativeStrengthIndPercentageChange, EMAfastPercentageChange, EMAslowPercentageChange) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)".format(self.tablename), (Close[i], High[i], Low[i], Open[i], VolumeFrom[i], VolumeTo[i], str(Timestamp[i]), str(Symbol[i]), str(ComparisonSymbol[i]), HighLowRatio[i], OpenCloseRatio[i], MovingAverage12h[i], ExpMovingAverage12h[i], RelativeStrengthIndex[i], EMAfast[i], EMAslow[i], ClosePercentageChange[i], HighPercentageChange[i], LowPercentageChange[i], OpenPercentageChange[i], VolumeFromPercentageChange[i], VolumeToPercentageChange[i], HighLowRatioPercentageChange[i], OpenCloseRatioPercentageChange[i], MovingAverage12hPercentageChange[i], ExpMovingAverage12hPercentageChange[i], RelativeStrengthIndPercentageChange[i], EMAfastPercentageChange[i], EMAslowPercentageChange[i]))
        elif self.tablename == 'CryptoDataHourlyEnrichedLabelled':
            Labels = UpdateDf['Labels']
            for i in range(0, len(Symbol)):
                self.c.execute("INSERT INTO {} (Close, High, Low, Open, VolumeFrom, VolumeTo, Timestamp, Symbol, ComparisonSymbol, HighLowRatio, OpenCloseRatio, MovingAverage12h, ExpMovingAverage12h, RelativeStrengthIndex, EMAfast, EMAslow, ClosePercentageChange, HighPercentageChange, LowPercentageChange, OpenPercentageChange, VolumeFromPercentageChange, VolumeToPercentageChange, HighLowRatioPercentageChange, OpenCloseRatioPercentageChange, MovingAverage12hPercentageChange, ExpMovingAverage12hPercentageChange, RelativeStrengthIndPercentageChange, EMAfastPercentageChange, EMAslowPercentageChange, Labels) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)".format(self.tablename), (Close[i], High[i], Low[i], Open[i], VolumeFrom[i], VolumeTo[i], str(Timestamp[i]), str(Symbol[i]), str(ComparisonSymbol[i]), HighLowRatio[i], OpenCloseRatio[i], MovingAverage12h[i], ExpMovingAverage12h[i], RelativeStrengthIndex[i], EMAfast[i], EMAslow[i], ClosePercentageChange[i], HighPercentageChange[i], LowPercentageChange[i], OpenPercentageChange[i], VolumeFromPercentageChange[i], VolumeToPercentageChange[i], HighLowRatioPercentageChange[i], OpenCloseRatioPercentageChange[i], MovingAverage12hPercentageChange[i], ExpMovingAverage12hPercentageChange[i], RelativeStrengthIndPercentageChange[i], EMAfastPercentageChange[i], EMAslowPercentageChange[i], Labels[i]))
        self.conn.commit()
        self.c.close()
        self.conn.close()
    