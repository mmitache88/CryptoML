# -*- coding: utf-8 -*-
"""
Created on Sat Oct 13 00:52:07 2018

@author: mmita
"""

import pandas as pd
import numpy as np
#%%
master = pd.read_csv("D:\\Google Drive\\1 Desktop\\Projects\\Cryptos\\Datasets\\price related\\sqlite\\CryptoDataHourlyEnrichedLabelled.csv")
#%%

#%%
master_mod = master.drop_duplicates(subset=["Close","High","Low","Open","VolumeFrom","VolumeTo","Timestamp","Symbol","ComparisonSymbol","OpenCloseRatio","OpenCloseRatioPercentageChange"])

#%%
master_mod.to_csv("D:\\Google Drive\\1 Desktop\\Projects\\Cryptos\\Datasets\\price related\\sqlite\\CryptoDataHourlyEnrichedLabelled_mod.csv")

#%%
master_original = pd.read_csv("D:\\Google Drive\\1 Desktop\\Projects\\Cryptos\\Datasets\\price related\\sqlite\\CryptoDataHourly.csv")
