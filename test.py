import pandas as pd
import numpy as np

# Sample series with NaN values
data = [1, np.nan, 2, np.nan, np.nan, 3, 4, np.nan, 5, 6, np.nan, np.nan, 7, 8, np.nan, 9, np.nan, 10, 11, np.nan, 12, 13, np.nan, np.nan, 14, 15, 16, np.nan, 17, 18, np.nan, np.nan, 19, 20, 21, np.nan, np.nan, 22, 23, np.nan, 24, 25, np.nan, 26, 27, 28, 29, np.nan, 30, 31]
series = pd.Series(data)

# Fill NaN values with the previous value
filled_series = series.fillna(method='ffill')

print(filled_series)
