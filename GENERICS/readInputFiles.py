import pandas as pd
import json
import os
import traceback

import logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)-23s %(levelname)-8s %(message)s [%(funcName)s: %(lineno)d]")
logger = logging.getLogger(__name__)

STRATEGY_FILE_PATH = "STRATEGY_FILE_PATH"
FUNCTIONS_FILE_PATH = "FUNCTIONS_FILE_PATH"
CAPITAL_RISK_FILE_PATH = "CAPITAL_RISK_FILE_PATH"


script_dir = os.path.dirname(os.path.realpath(__file__))
file_path_json = os.path.join(script_dir, "filepath.json")
##print(file_path_json)

input_file_path = None
with open(file_path_json, "r") as file:
    # Read the contents of the file
    input_file_path = json.load(file)
##    print(input_file_path)

def __init__(self) -> None:
    try:
        self.strategy_df = None
        self.functions_df = None
        self.capitalrisk_df = None
    except Exception as ex:
        logger.exception(traceback.format_exc())
        
def read_strategy_file(self):
    try:
        if STRATEGY_FILE_PATH in input_file_path:
            file_path = input_file_path[STRATEGY_FILE_PATH]
            self.strategy_df = pd.read_csv(file_path)
            return True
        else:
            return False           
    except Exception as ex:
        logger.exception(traceback.format_exc())

def read_functions_file(self):
    try:
        if FUNCTIONS_FILE_PATH in input_file_path:
            file_path = input_file_path[FUNCTIONS_FILE_PATH]
            self.functions_df = pd.read_csv(file_path)
            return True
        else:
            return False           
    except Exception as ex:
        logger.exception(traceback.format_exc())

def read_capitalrisk_file(self):
    try:
        if CAPITAL_RISK_FILE_PATH in input_file_path:
            file_path = input_file_path[CAPITAL_RISK_FILE_PATH]
            self.capitalrisk_df = pd.read_csv(file_path)
            return True
        else:
            return False           
    except Exception as ex:
        logger.exception(traceback.format_exc())
