from GENERICS import GlobalVariables
import pandas as pd
import numpy as np
import logging
logging.basicConfig(level=logging.info, format="%(asctime)-23s %(levelname)-8s %(message)s [%(funcName)s: %(lineno)d]")
logger = logging.getLogger(__name__)

def calculate_fractal(data:pd.DataFrame,fractal_period=2):
    # Convert DataFrame to dictionary
    data['key'] = data['dt']
    data = data.set_index('key').T.to_dict()
    final_result = {}
    bars_collection = []
    try:
        period = fractal_period
        # fix_period = (period - 2) + 9
        fix_period = (period * 2) + 5
        for key, bar in data.items():
            bar = set_default_values(bar)
            bars_collection.append(bar)
            if len(bars_collection) == fix_period:
                temp_bar_collection = bars_collection[::-1]
                downflag = downflag0 = downflag1 = downflag2 = downflag3 = downflag4 = True
                for i in range(1,period+1):
                    downflag = downflag and temp_bar_collection[period-i][GlobalVariables.OHLC_LOW] > temp_bar_collection[period][GlobalVariables.OHLC_LOW]
                    downflag0 = downflag0 and temp_bar_collection[period+i][GlobalVariables.OHLC_LOW] > temp_bar_collection[period][GlobalVariables.OHLC_LOW]
                    downflag1 = downflag1 and temp_bar_collection[period+1][GlobalVariables.OHLC_LOW] >= temp_bar_collection[period][GlobalVariables.OHLC_LOW] and temp_bar_collection[period+i+1][GlobalVariables.OHLC_LOW] > temp_bar_collection[period][GlobalVariables.OHLC_LOW]
                    downflag2 = downflag2 and temp_bar_collection[period+1][GlobalVariables.OHLC_LOW] >= temp_bar_collection[period][GlobalVariables.OHLC_LOW] and temp_bar_collection[period+2][GlobalVariables.OHLC_LOW] >= temp_bar_collection[period][GlobalVariables.OHLC_LOW] and temp_bar_collection[period+i+2][GlobalVariables.OHLC_LOW] > temp_bar_collection[period][GlobalVariables.OHLC_LOW]
                    downflag3 = downflag3 and temp_bar_collection[period+1][GlobalVariables.OHLC_LOW] >= temp_bar_collection[period][GlobalVariables.OHLC_LOW] and temp_bar_collection[period+2][GlobalVariables.OHLC_LOW] >= temp_bar_collection[period][GlobalVariables.OHLC_LOW] and temp_bar_collection[period+3][GlobalVariables.OHLC_LOW] >= temp_bar_collection[period][GlobalVariables.OHLC_LOW] and temp_bar_collection[period+i+3][GlobalVariables.OHLC_LOW] > temp_bar_collection[period][GlobalVariables.OHLC_LOW]
                    downflag4 = downflag4 and temp_bar_collection[period+1][GlobalVariables.OHLC_LOW] >= temp_bar_collection[period][GlobalVariables.OHLC_LOW] and temp_bar_collection[period+2][GlobalVariables.OHLC_LOW] >= temp_bar_collection[period][GlobalVariables.OHLC_LOW] and temp_bar_collection[period+3][GlobalVariables.OHLC_LOW] >= temp_bar_collection[period][GlobalVariables.OHLC_LOW] and temp_bar_collection[period+4][GlobalVariables.OHLC_LOW] >= temp_bar_collection[period][GlobalVariables.OHLC_LOW]and temp_bar_collection[period+i+4][GlobalVariables.OHLC_LOW] > temp_bar_collection[period][GlobalVariables.OHLC_LOW]
                flagDown = downflag0 or downflag1 or downflag2 or downflag3 or downflag4
                if downflag and flagDown:
                    bar_2 = temp_bar_collection[period]
                    bar_2[GlobalVariables.FRACTAL_LONG] = True # green
                    bar_0 = temp_bar_collection[0]
                    bar_0[GlobalVariables.FRACTAL_CONFIRMED_LONG] = True
                    bar_0[GlobalVariables.FRACTAL_TAG_DT] = bars_collection[period][GlobalVariables.OHLC_DT]
                # GREEN FRACTAL
                upflag = upflag0 = upflag1 = upflag2 = upflag3 = upflag4 = True
                for i in range(1,period+1):
                    upflag = upflag and temp_bar_collection[period-i][GlobalVariables.OHLC_HIGH] < temp_bar_collection[period][GlobalVariables.OHLC_HIGH]
                    upflag0 = upflag0 and temp_bar_collection[period+i][GlobalVariables.OHLC_HIGH] < temp_bar_collection[period][GlobalVariables.OHLC_HIGH]
                    upflag1 = upflag1 and temp_bar_collection[period+1][GlobalVariables.OHLC_HIGH] <= temp_bar_collection[period][GlobalVariables.OHLC_HIGH] and temp_bar_collection[period+i+1][GlobalVariables.OHLC_HIGH] < temp_bar_collection[period][GlobalVariables.OHLC_HIGH]
                    upflag2 = upflag2 and temp_bar_collection[period+1][GlobalVariables.OHLC_HIGH] <= temp_bar_collection[period][GlobalVariables.OHLC_HIGH] and temp_bar_collection[period+2][GlobalVariables.OHLC_HIGH] <= temp_bar_collection[period][GlobalVariables.OHLC_HIGH] and temp_bar_collection[period+i+2][GlobalVariables.OHLC_HIGH] < temp_bar_collection[period][GlobalVariables.OHLC_HIGH]
                    upflag3 = upflag3 and temp_bar_collection[period+1][GlobalVariables.OHLC_HIGH] <= temp_bar_collection[period][GlobalVariables.OHLC_HIGH] and temp_bar_collection[period+2][GlobalVariables.OHLC_HIGH] <= temp_bar_collection[period][GlobalVariables.OHLC_HIGH] and temp_bar_collection[period+3][GlobalVariables.OHLC_HIGH] <= temp_bar_collection[period][GlobalVariables.OHLC_HIGH] and temp_bar_collection[period+i+3][GlobalVariables.OHLC_HIGH] < temp_bar_collection[period][GlobalVariables.OHLC_HIGH]
                    upflag4 = upflag4 and temp_bar_collection[period+1][GlobalVariables.OHLC_HIGH] <= temp_bar_collection[period][GlobalVariables.OHLC_HIGH] and temp_bar_collection[period+2][GlobalVariables.OHLC_HIGH] <= temp_bar_collection[period][GlobalVariables.OHLC_HIGH] and temp_bar_collection[period+3][GlobalVariables.OHLC_HIGH] <= temp_bar_collection[period][GlobalVariables.OHLC_HIGH] and temp_bar_collection[period+4][GlobalVariables.OHLC_HIGH] <= temp_bar_collection[period][GlobalVariables.OHLC_HIGH]and temp_bar_collection[period+i+4][GlobalVariables.OHLC_HIGH] < temp_bar_collection[period][GlobalVariables.OHLC_HIGH]
                flagUp = upflag0 or upflag1 or upflag2 or upflag3 or upflag4
                if upflag and flagUp:
                    bar_2 = temp_bar_collection[period]
                    bar_2[GlobalVariables.FRACTAL_SHORT] = True # green
                    bar_0 = temp_bar_collection[0]
                    bar_0[GlobalVariables.FRACTAL_CONFIRMED_SHORT] = True
                    bar_0[GlobalVariables.FRACTAL_TAG_DT] = bars_collection[period][GlobalVariables.OHLC_DT]
                temp_bar = bars_collection.pop(0)
                final_result[temp_bar[GlobalVariables.OHLC_DT]] = temp_bar
        df = pd.DataFrame(final_result.values())
        df = fractal_high_low(df)
        return df
    except Exception as ex:
        logging.exception(ex)

def fractal_high_low(df:pd.DataFrame):
    # Initialize new columns
    df['fractal_low'] = np.nan
    df['fractal_high'] = np.nan

    # Apply conditions
    df.loc[df[ GlobalVariables.FRACTAL_LONG], 'fractal_low'] = df['l']
    df.loc[df[ GlobalVariables.FRACTAL_SHORT], 'fractal_high'] = df['h']
    return df

def calculate_rolling_average(df:pd.DataFrame,window,tag1,tag2):
    windows_data = []
    data = []
    for index,row in df.iterrows():
        if not str(row[tag1]) == str(np.nan):
            windows_data.append(row[tag1])
        if  row[tag2]  and len(windows_data) == window:
            avg = round(sum(windows_data) / len(windows_data),2)
            data.append(avg)
            windows_data.pop(0)
        else:
            data.append(np.nan)
    return data

def set_default_values(bar:dict):
    temp = {GlobalVariables.PRE_FRACTAL_LONG:False, GlobalVariables.FRACTAL_LONG:False, GlobalVariables.FRACTAL_CONFIRMED_LONG:False
                                  ,GlobalVariables.PRE_FRACTAL_SHORT:False, GlobalVariables.FRACTAL_SHORT:False, GlobalVariables.FRACTAL_CONFIRMED_SHORT:False
                                  , GlobalVariables.FRACTAL_TAG_DT:""}
    bar.update(temp)
    return bar
