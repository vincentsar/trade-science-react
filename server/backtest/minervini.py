'''
General csv database based on Minervini's strategy
'''

import pandas as pd
import os
from backtest.trading import *

###################### SETUP ######################
# General trade setup
TR_ATR_N = 14

# Transition long setup
TR_HIGHERHIGH_LOWERLOW_AMOUNTS = 5
TR_HIGHERHIGH_LOWERLOW_PERIODS = 30

TR_VOLUMESPIKE_RATIO = 1.3
TR_VOLUMESPIKE_AMOUNTS = 3
TR_VOLUMESPIKE_PERIODS = 30

WEEKTR_VOLUME_UP_LT_DOWN_PERIODS = 5  # In weeks (before conversion back to days)

TR_VMA = 50

###################### PANDAS RELATED FUNCTION ######################
def dfSetupMinervini(df):
    # Calculate default variables that we'll typically use 
    df['ATR'] = df_atr(df, n=TR_ATR_N)
    df['returns'] = df_return(df)
    df[f'VMA_{TR_VMA}'] = df_vma(df, TR_VMA)
    df['SMA_50'] = df_sma(df, 50)
    df['SMA_150'] = df_sma(df, 150)
    df['SMA_200'] = df_sma(df, 200)
    df.dropna(inplace = True)

    # Calculate Minervini Criterias
    df['long_sma'] = dfMinerviniTransitionLongCriteria_1(df)
    df['long_hhhl'] = dfMinerviniTransitionLongCriteria_2(df)
    
    # Volume based: We need to use front end to check first
    df = dfMinerviniTransitionLongCriteria_3(df)
    
    return df

###################### MINERVINI TRANSITION LONG ######################
def dfMinerviniTransitionLongCriteria_1(df):
    '''
    Simple comparison
    1. The stock price is above both the 150-day and the 200-day moving average.
    2. The 150-day moving average is above the 200-day.
    3. The 200-day moving average has turned up.
    '''
    criteria1 = (df['close'] > df['SMA_150']) & (df['close'] > df['SMA_200'])
    criteria2 = df['SMA_150'] > df['SMA_200']
    criteria3 = df['SMA_200'] > df['SMA_200'].shift()

    # All criteria
    all_criteria = criteria1 & criteria2 & criteria3

    return all_criteria

def dfMinerviniTransitionLongCriteria_2(df, hhhl_amounts=TR_HIGHERHIGH_LOWERLOW_AMOUNTS, hhhl_periods=TR_HIGHERHIGH_LOWERLOW_PERIODS):
    '''
    4. A series of higher highs and higher lows has occurred.
    - seriesofhigherhighs = higher highs occured at least X times within last X days
    - seriesofhigherlows = higher lows occured at least X times within last X days
    '''
    # Criteria 4: A series of higher highs and higher lows has occurred
    higherhighs = df['high'] > df['high'].shift()
    higherlows = df['low'] > df['low'].shift()
    seriesofhigherhighs = higherhighs.rolling(window=hhhl_periods).sum() >= hhhl_amounts
    seriesofhigherlows = higherlows.rolling(window=hhhl_periods).sum() >= hhhl_amounts
    
    criteria4 = seriesofhigherhighs & seriesofhigherlows

    return criteria4

def dfMinerviniTransitionLongCriteria_3(df, volume_spike=TR_VOLUMESPIKE_RATIO, spike_amounts=TR_VOLUMESPIKE_AMOUNTS, spike_periods=TR_VOLUMESPIKE_PERIODS, weeklyvolume_up_lt_down_periods=WEEKTR_VOLUME_UP_LT_DOWN_PERIODS):
    '''
    @return DF with new columns
    5. Large up weeks on volume spikes are contrasted by low-volume pullbacks.
    6. There are more up weeks on volume than down weeks on volume.
    '''

    # Criteria 5: Large up weeks on volume spikes are contrasted by low-volume pullbacks
    volume_spike = df['volume'] > (volume_spike * df[f'VMA_{TR_VMA}'])
    seriesofvolumespikes = volume_spike.rolling(window=spike_periods).sum() >= spike_amounts
    df['volumespike'] = volume_spike
    df['long_vspike'] = seriesofvolumespikes

    # Criteria 6: There are more up weeks on volume than down weeks on volume
    df['pastweek_volume'] = df['volume'].rolling(window=7).sum()
    df['pastweek_return'] = df['pastweek_volume'].pct_change()
    df['pastweek_volume_up'] = df['pastweek_return'] > 0
    df['pastweek_volume_down'] = df['pastweek_return'] < 0
    df['period_up_week'] = df['pastweek_volume_up'].rolling(window=weeklyvolume_up_lt_down_periods).sum()
    df['period_down_week'] = df['pastweek_volume_down'].rolling(window=weeklyvolume_up_lt_down_periods).sum()
    df['long_week_vup_lt_vdn'] = df['period_up_week'] > df['period_down_week']

    return df
