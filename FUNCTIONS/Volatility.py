import pandas as pd
import numpy as np
import math

# Function to calculate the change (E)
def calculate_change(close_prices):
    """
    Calculate the ratio of current day's closing price to the previous day's closing price.
    
    Parameters:
    close_prices (pd.Series): Series of daily closing prices.
    
    Returns:
    pd.Series: Series containing the calculated change ratios to log
    """
    percentage_change = close_prices / close_prices.shift(1) - 1
    # Convert percentage change to logarithmic returns
    log_returns = percentage_change.apply(lambda x: math.log(1 + x)) * 10
    return log_returns

# Function to calculate standard deviation (F)
def calculate_stdv(changes, n,stdv_neg):
    """
    Calculate the standard deviation of changes in closing prices over a rolling window of N days.
    
    Parameters:
    changes (pd.Series): Series of daily change ratios.
    n (int): Number of days for the rolling window.
    
    Returns:
    pd.Series: Series containing the rolling standard deviation values.
    """
    if stdv_neg:
        # Create an empty series to store the rolling standard deviation
        rolling_stdv = pd.Series(index=changes.index, dtype=float)
        
        # Iterate over the series to calculate the rolling standard deviation
        for i in range(len(changes)):
            if i >= n - 1:
                window = changes[i - n + 1:i + 1]  # Get the current rolling window
                negative_window = window[window < 0]  # Consider only negative changes
                if len(negative_window) > 0:
                    rolling_stdv[i] = negative_window.std(ddof=0)  # Calculate standard deviation
                else:
                    rolling_stdv[i] = np.nan  # If no positive changes, set as NaN
            else:
                rolling_stdv[i] = np.nan  # Not enough data for a full window
        
        return rolling_stdv

    return changes.rolling(window=n).std(ddof=0)

# Function to calculate annualized volatility (G)
def calculate_annualized_volatility(stdv, trading_days=252):
    """
    Calculate the annualized volatility from the standard deviation of changes.
    
    Parameters:
    stdv (pd.Series): Series of rolling standard deviations.
    trading_days (int): Number of trading days in a year, default is 252.
    
    Returns:
    pd.Series: Series containing the annualized volatility values.
    """
    return stdv * np.sqrt(trading_days)

# Function to calculate average of annualized volatility (H)
def calculate_avg_volatility(annualized_volatility, n):
    """
    Calculate the average of annualized volatility over a rolling window of N days.
    
    Parameters:
    annualized_volatility (pd.Series): Series of annualized volatility values.
    n (int): Number of days for the rolling window.
    
    Returns:
    pd.Series: Series containing the rolling average of annualized volatility.
    """
    return annualized_volatility.rolling(window=n).mean()


# Example usage:
# Assuming `df` is your DataFrame containing the daily closing prices:
# stdv_rolling_period = 0
# avg_volatility_rolling_peirod = 0
# fileName = ""
# df = pd.read_csv(fileName) ## add file to to read
# df['calculate_change'] = calculate_change(df['D'])
# df['calculate_stdv'] = calculate_stdv(df['calculate_change'], stdv_rolling_period)
# df['calculate_annualized_volatility'] = calculate_annualized_volatility(df['calculate_stdv'])
# df['calculate_avg_volatility'] = calculate_avg_volatility(df['calculate_annualized_volatility'], avg_volatility_rolling_peirod)
# df.to_csv(f"output_{fileName}")
