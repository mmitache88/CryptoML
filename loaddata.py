# -*- coding: utf-8 -*-
"""
Created on Mon Jan 22 20:48:48 2018

@author: mmita
"""

import sqlite3
import pandas as pd
import os.path

class Load_Data_Class:
    def __init__(self,path,dbName):
        db_path = os.path.join (path, dbName)
        self.conn= sqlite3.connect(db_path)
        
    def read_from_db (self, tablename):
        self.c = self.conn.cursor()
        self.c.execute('SELECT * FROM {}'.format(tablename))
        MasterDf = pd.DataFrame(self.c.fetchall())
        if MasterDf.shape[1] < 15:
            MasterDf.columns = ['Close', 'High', 'Low', 'Open', 'VolumeFrom', 'VolumeTo', 'Timestamp', 'Symbol', 'ComparisonSymbol']
        elif MasterDf.shape[1] == 29:
            MasterDf.columns = ['Close', 'High', 'Low', 'Open', 'VolumeFrom', 'VolumeTo', 'Timestamp', 'Symbol', 'ComparisonSymbol','HighLowRatio','OpenCloseRatio','MovingAverage12h','ExpMovingAverage12h','RelativeStrengthIndex','EMAfast','EMAslow','ClosePercentageChange','HighPercentageChange','LowPercentageChange','OpenPercentageChange','VolumeFromPercentageChange','VolumeToPercentageChange','HighLowRatioPercentageChange','OpenCloseRatioPercentageChange','MovingAverage12hPercentageChange','ExpMovingAverage12hPercentageChange','RelativeStrengthIndPercentageChange','EMAfastPercentageChange','EMAslowPercentageChange']
        else:
            MasterDf.columns = ['Close', 'High', 'Low', 'Open', 'VolumeFrom', 'VolumeTo', 'Timestamp', 'Symbol', 'ComparisonSymbol','HighLowRatio','OpenCloseRatio','MovingAverage12h','ExpMovingAverage12h','RelativeStrengthIndex','EMAfast','EMAslow','ClosePercentageChange','HighPercentageChange','LowPercentageChange','OpenPercentageChange','VolumeFromPercentageChange','VolumeToPercentageChange','HighLowRatioPercentageChange','OpenCloseRatioPercentageChange','MovingAverage12hPercentageChange','ExpMovingAverage12hPercentageChange','RelativeStrengthIndPercentageChange','EMAfastPercentageChange','EMAslowPercentageChange','Labels']
        return MasterDf
    