import pandas as pd 
import numpy as np

###################### COMMON FUNCTIONS ######################
def df_wwma(values, n):
    '''
    J. Welles Wilder's EMA 
    '''
    return values.ewm(alpha=1/n, min_periods=n, adjust=False).mean()

def df_atr(df, n=14):
    '''
    Average True Range for the measurement of volatility
    '''
    high = df['high']
    low = df['low']
    close = df['close']
    tr0 = abs(high - low)
    tr1 = abs(high - close.shift())
    tr2 = abs(low - close.shift())
    tr = pd.concat([tr0, tr1, tr2], axis=1).max(axis=1)
    atr = df_wwma(tr, n)
    return atr

def df_return(df):
    returns = np.log(df['close'].div(df['close'].shift(1)))
    return returns

def df_sma(df, window):
    sma = df['close'].rolling(window=window).mean()
    return sma

def df_vma(df, window):
    vma = df['volume'].rolling(window=window).mean()
    return vma

###################### MINERVINI FUNCTIONS ######################
