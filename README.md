
## COMMON

strategyId                          : int 
parameterId	                        : int
instrument	                        : str
tf                                  : int (timeframe in minutes) 

## VOLATILITY

function                            : str (value VOLATILITY)
stdv_rolling_period                 : int
stdv_neg                            : False
avg_volatility_rolling_peirod       : int

## VOLUME

function                            : str (value VOLUME)
zscore_sum_rolling_period           : int
zscore_sum_avg_rolling_period       : int

## run cmd 
py run.py -s "ABBOTINDIA"