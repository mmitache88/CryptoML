# -*- coding: utf-8 -*-
"""
Created on Mon Jan 22 21:34:28 2018

@author: mmita
"""
import numpy as np
import pandas as pd

class GenerateFeaturesClass:
    
    def __init__ (self):
        pass   
    
    def movingaverage(self, values, window):
        weights = np.repeat(1.0, window)/window
        sma = np.convolve(values,weights,'valid')
        return sma
    
    def rsiFunc(self, prices, n=14):
        """
        The Relative Strength Index (RSI) is a momentum oscillator that measures the speed and change of 
        price movements. The RSI oscillates between zero and 100. Traditionally the RSI is considered 
        overbought when above 70 and oversold when below 30. 
        """
        deltas = np.diff(prices)
        seed = deltas[:n+1]
        up = seed[seed>=0].sum()/n
        down = -seed[seed<0].sum()/n
        rs = up/down
        rsi = np.zeros_like(prices)
        rsi[:n] = 100. - 100./(1.+rs)
        
        for i in range(n, len(prices)):
            delta = deltas[i-1] # cause the diff is 1 shorter
            
            if delta>0:
                upval = delta
                downval = 0.
            else:
                upval = 0.
                downval = -delta
                
            up = (up*(n-1) + upval)/n
            down = (down*(n-1) + downval)/n
        
            rs = up/down
            rsi[i] = 100. - 100./(1. + rs)
            
        return rsi
    
    def ExpMovingAverage(self, values, window):
        weights = np.exp(np.linspace(-1., 0., window))
        weights /= weights.sum()
        a =  np.convolve(values, weights, mode='full')[:len(values)]
        a[:window] = a[window]
        
        return a    
    
    def computeMACD(self, x, slow=26, fast=12):
        """
        compute the MACD (Moving Average Convergence/Divergence) using a fast and slow exponential moving avg
        return value is emaslow, emafast, macd which are len(x) arrays
        """
        emaslow = self.ExpMovingAverage(x, slow)
        emafast = self.ExpMovingAverage(x, fast)
        return emaslow, emafast, emafast - emaslow   
    
    def percentageDif(self, x):
        y = (np.diff(x)/x[:-1])*100
        return y
    
