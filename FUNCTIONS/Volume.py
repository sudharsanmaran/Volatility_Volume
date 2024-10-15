import pandas as pd
import numpy as np

# Function to calculate standard deviation of volume (D)
def calculate_volume_stdv(volume):
    """
    Calculate the standard deviation of volume on a cumulative basis from the start to the current day.
    
    Parameters:
    volume (pd.Series): Series of daily trading volumes.
    
    Returns:
    pd.Series: Series containing the cumulative standard deviation values of volume.
    """
    return volume.expanding().std(ddof=0)

# Function to calculate average volume (E)
def calculate_avg_volume(volume):
    """
    Calculate the average trading volume over a rolling window of N days.
    
    Parameters:
    volume (pd.Series): Series of daily trading volumes.
    n (int): Number of days for the rolling window.
    
    Returns:
    pd.Series: Series containing the rolling average volume values.
    """
    # return volume.rolling(window=n).mean()
    return volume.expanding().mean()

# Function to calculate Z-score of volume (F)
def calculate_volume_zscore(volume, avg_volume, volume_stdv):
    """
    Calculate the Z-score of the current day's volume.
    
    Parameters:
    volume (pd.Series): Series of daily trading volumes.
    avg_volume (pd.Series): Series of rolling average volume values.
    volume_stdv (pd.Series): Series of cumulative standard deviation values of volume.
    
    Returns:
    pd.Series: Series containing the Z-scores of volume.
    """
    return (volume - avg_volume) / volume_stdv

# Function to calculate sum of Z-scores (G)
def calculate_sum_zscores(z_scores, n):
    """
    Calculate the sum of Z-scores over a rolling window of N days.
    
    Parameters:
    z_scores (pd.Series): Series of Z-scores of volume.
    n (int): Number of days for the rolling window.
    
    Returns:
    pd.Series: Series containing the rolling sum of Z-scores.
    """
    return z_scores.rolling(window=n).sum()

# Function to calculate average of Z-score sums (H)
def calculate_avg_zscore_sums(sum_zscores, n):
    """
    Calculate the average of Z-score sums over a rolling window of N days.
    
    Parameters:
    sum_zscores (pd.Series): Series of sums of Z-scores.
    n (int): Number of days for the rolling window.
    
    Returns:
    pd.Series: Series containing the rolling average of Z-score sums.
    """
    return sum_zscores.rolling(window=n).mean()

# Example usage:
# Assuming `df` is your DataFrame containing the daily trading volumes:
# df['D'] = calculate_volume_stdv(df['C'])
# df['E'] = calculate_avg_volume(df['C'], N)
# df['F'] = calculate_volume_zscore(df['C'], df['E'], df['D'])
# df['G'] = calculate_sum_zscores(df['F'], N)
# df['H'] = calculate_avg_zscore_sums(df['G'], N)

# Logging the resulting DataFrame
# log_df(df)
