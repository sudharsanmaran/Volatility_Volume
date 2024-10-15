import pandas as pd




def changePer_rolling_avg_tagging(df,rolling_period):
    # Calculate percentage change
    df['Pct_Change'] = df['Value'].pct_change() * 100

    # Define rolling period
    rolling_period = 3

    # Calculate rolling average of percentage change
    df['Rolling_Avg'] = df['Pct_Change'].rolling(window=rolling_period).mean()

    # Determine color based on rolling average
    df['Color'] = df['Rolling_Avg'].apply(lambda x: 'GREEN' if x > 0 else 'RED')

    return df