import json
import os
import traceback
from datetime import datetime, timedelta
import pandas as pd


import logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)-23s %(levelname)-8s %(message)s [%(funcName)s: %(lineno)d]")
logger = logging.getLogger(__name__)

#SPOT DATA fileName formate : <SYMBOL>_<TF>.<csv.json>

SPOT_DATA = "SPOT_DATA"
EXISTING_SPOT_DATA_TIMEFRAME = "EXISTING_SPOT_DATA_TIMEFRAME"
FUT_DATA = "FUT_DATA"
OPT_DATA = "OPT_DATA"
PCR_DATA = "PCR_DATA"


script_dir = os.path.dirname(os.path.realpath(__file__))
file_path_json = os.path.join(script_dir, "filepath.json")

data_file_path = None
with open(file_path_json, "r") as file:
    # Read the contents of the file
    data_file_path = json.load(file)

class readData():
    def __init__(self) -> None:
        try:
            self.spot_data_path = ""
            self.fut_data_path = ""
            self.opt_data_path = ""
            self.opt_pcr_data = ""
        except Exception as ex:
            logger.exception("DATA.readData.__init__",traceback.format_exc())

    def read_spot_data(self,symbol,start_dt,end_dt):
        try:
            if SPOT_DATA in data_file_path:
                file_path = data_file_path[SPOT_DATA]
                temp_data = self.load_historydata_as_json(start_dt,end_dt,symbol,file_path)
                return temp_data
            else:
                return None
        except Exception as ex:
            logger.exception("DATA.readData.read_spot_data",traceback.format_exc())

    def read_opt_pcr_data(self,symbol):
        try:
            if PCR_DATA in data_file_path:
                file_path = data_file_path[PCR_DATA]
                file_path = f"{file_path}\\{symbol}.csv"
                if os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                    return df
            return None
        except Exception as ex:
            logger.exception("DATA.readData.read_opt_pcr_data",traceback.format_exc())

    def load_historydata_as_json(self,start_dt,end_dt,symbol,file_path): # SPOT Data
        try:
            symbol_data = {}
            sTime = datetime.now()
            path = file_path
            flag = True
            dt = start_dt
            while(flag):
                if dt > end_dt:
                    break
                if dt.date() in [datetime(2013,5,11).date()]:
                    dt = dt + timedelta(days=1)
                    continue
                year = dt.year
                day_month = dt.strftime("%b")
                name = dt.strftime("%d%m%Y")
                if not dt.date() in symbol_data:
                    file = f'{path}\\{symbol}\\{year}\\{str(day_month).upper()}\\GFDLNFO_MINUTE_{symbol.upper()}_SPOT_{name}.json'
                    if os.path.exists(file):
                        if os.stat(file).st_size == 0:
                            dt = dt + timedelta(days=1)
                            continue
                        with open(file) as f:
                            symbol_data[dt.date()] = json.load(f)
                dt = dt + timedelta(days=1)
            time_delta = (datetime.now() - sTime).total_seconds()
            return symbol_data
            # print("loading json data completed in : " + str(time_delta) + "for "+symbol)
        except Exception as ex:
            logger.exception("DATA.readData.load_historydata_as_json",traceback.format_exc())