import argparse
import multiprocessing
import traceback
from datetime import datetime, timedelta
from GENERICS import readInputFiles
import os
import pandas as pd
import numpy as np
from DATA.readData import readData
from GENERICS import GlobalVariables
from FUNCTIONS import Volatility, Volume, Fractal
import logging

from utils import write_dataframe_to_csv
from validations import Strategy

logging.basicConfig(
    level=logging.info,
    format="%(asctime)-23s %(levelname)-8s %(message)s [%(funcName)s: %(lineno)d]",
)
logger = logging.getLogger(__name__)

annualized_volatility_sqrt = {
    60: 1764,
    120: 1008,
    240: 504,
    375: 252,
    1125: 84,
}

BASE_TIMEFRAME = 1  # 1 minute is the base time frame data wil load

BT_START_DT = datetime(2013, 1, 1)
BT_END_DT = datetime(2024, 6, 30)

MARKET_START_TIME = datetime(1970, 1, 1, 9, 15)
MARKET_END_TIME = datetime(1970, 1, 1, 15, 30)

OPEN = "Open"
HIGH = "High"
LOW = "Low"
CLOSE = "Close"
VOLUME = "Volume"


def read_strategy_file(fileToRead):
    try:
        if not os.path.exists(fileToRead):
            logging.info(f"Strategy input file not found {fileToRead}")
            return None
        temp = pd.read_csv(fileToRead)
        return temp
    except Exception as ex:
        logger.exception(traceback.format_exc())


def convert_seconds_to_time(seconds):
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    return hour, minutes, seconds


def process_json_data(data):
    try:
        unique_dt = []
        default_dt = datetime(1970, 1, 1)
        bars_collection = {}
        for dt, day in data.items():
            DtInDays = (dt - default_dt.date()).days
            temp_data = day[str(DtInDays)]
            for tm, bar in temp_data.items():
                hour, minutes, seconds = convert_seconds_to_time(int(tm))
                new_dt = datetime(dt.year, dt.month, dt.day, hour, minutes, 0)
                if new_dt.time() < MARKET_START_TIME.time():
                    continue
                if new_dt.time() >= MARKET_END_TIME.time():
                    continue
                if new_dt in bars_collection:
                    continue
                open_ = float(bar[OPEN])
                high = float(bar[HIGH])
                low = float(bar[LOW])
                close_ = float(bar[CLOSE])
                volume = int(bar[VOLUME]) if VOLUME in bar else 0
                bar_endTime = dt
                # objBar.volume = int(bar["TradedQty"])
                bars_collection[new_dt] = {
                    GlobalVariables.OHLC_DT: new_dt,
                    GlobalVariables.OHLC_OPEN: open_,
                    GlobalVariables.OHLC_HIGH: high,
                    GlobalVariables.OHLC_LOW: low,
                    GlobalVariables.OHLC_CLOSE: close_,
                    GlobalVariables.OHLC_END_DT: bar_endTime,
                    GlobalVariables.OHLC_VOLUME: volume,
                }
                if not new_dt.date() in unique_dt:
                    unique_dt.append(new_dt.date())
        return bars_collection, unique_dt
    except Exception as ex:
        logger.exception(traceback.format_exc())


# trading view style bars start end time
def creating_tf_start_end_time(unique_dt, tf):
    try:
        datetime_collection = []
        for dt in unique_dt:
            temp_market_time = MARKET_START_TIME
            while True:
                new_dt = datetime(
                    dt.year,
                    dt.month,
                    dt.day,
                    temp_market_time.hour,
                    temp_market_time.minute,
                    temp_market_time.second,
                )
                if new_dt.time() >= MARKET_END_TIME.time():
                    break
                datetime_collection.append(new_dt)
                temp_market_time = temp_market_time + timedelta(
                    minutes=BASE_TIMEFRAME
                )
        GlobalVariables.datetime_collection = datetime_collection.copy()
        temp_collection = []
        startTime = None
        temp_marketETime = MARKET_END_TIME + timedelta(minutes=-1)
        bar_start_end_time = {}
        for dt in datetime_collection:
            if startTime is None:
                startTime = dt
            temp_collection.append(dt)
            if (
                len(temp_collection) == tf
                or dt.time() == temp_marketETime.time()
            ):
                # print(f" StartTime {startTime} - EndTime {dt}")
                bar_start_end_time[startTime] = temp_collection.copy()
                startTime = None
                temp_collection.clear()
        return bar_start_end_time
    except Exception as ex:
        logger.exception(traceback.format_exc())


def fill_missing_bars_to_base_timeframe(tf_start_end_time, data):
    try:
        prev_tm = None
        for tm in tf_start_end_time:
            if tm not in data and prev_tm is data:
                prev_data = data[prev_tm]
                close = prev_data["c"]
                data[tm] = {
                    "dt": tm,
                    "o": close,
                    "h": close,
                    "l": close,
                    "c": close,
                    "v": 0,
                }
            prev_tm = tm
        return data
    except Exception as ex:
        logger.exception(traceback.format_exc())


def volatility(strategy_dict: dict, data: pd.DataFrame):
    try:
        paramId = strategy_dict["parameterId"]
        stdv_rolling_period = strategy_dict["stdv_rolling_period"]
        stdv_neg = strategy_dict["stdv_neg"]
        avg_volatility_rolling_peirod = strategy_dict[
            "avg_volatility_rolling_peirod"
        ]
        sqrrt = annualized_volatility_sqrt[strategy_dict["tf"]]
        data[f"calculate_change_{paramId}"] = Volatility.calculate_change(
            data[GlobalVariables.OHLC_CLOSE]
        )
        data[f"calculate_stdv_{paramId}_{stdv_rolling_period}"] = (
            Volatility.calculate_stdv(
                data[f"calculate_change_{paramId}"],
                stdv_rolling_period,
                stdv_neg,
            )
        )
        data[f"calculate_annualized_volatility_{paramId}"] = (
            Volatility.calculate_annualized_volatility(
                data[f"calculate_stdv_{paramId}_{stdv_rolling_period}"], sqrrt
            )
        )
        data[
            f"calculate_avg_volatility_{paramId}_{avg_volatility_rolling_peirod}"
        ] = Volatility.calculate_avg_volatility(
            data[f"calculate_annualized_volatility_{paramId}"],
            avg_volatility_rolling_peirod,
        )
        data = tag_iv_data(data, strategy_dict["instrument"])
        return data
    except Exception as ex:
        logger.exception(traceback.format_exc())


def volume(strategy_dict: dict, data: pd.DataFrame):
    try:
        paramId = strategy_dict["parameterId"]
        zscore_sum_rolling_period = strategy_dict["zscore_sum_rolling_period"]
        zscore_sum_avg_rolling_period = strategy_dict[
            "zscore_sum_avg_rolling_period"
        ]
        data[f"calculate_volume_stdv_{paramId}"] = (
            Volume.calculate_volume_stdv(data[GlobalVariables.OHLC_VOLUME])
        )
        data[f"calculate_avg_volume_{paramId}"] = Volume.calculate_avg_volume(
            data[GlobalVariables.OHLC_VOLUME]
        )
        data[f"calculate_volume_zscore_{paramId}"] = (
            Volume.calculate_volume_zscore(
                data[GlobalVariables.OHLC_VOLUME],
                data[f"calculate_avg_volume_{paramId}"],
                data[f"calculate_volume_stdv_{paramId}"],
            )
        )
        data[
            f"calculate_sum_zscores_{paramId}_{zscore_sum_rolling_period}"
        ] = Volume.calculate_sum_zscores(
            data[f"calculate_volume_zscore_{paramId}"],
            zscore_sum_rolling_period,
        )
        data[
            f"calculate_avg_zscore_sums_{paramId}_{zscore_sum_rolling_period}"
        ] = Volume.calculate_avg_zscore_sums(
            data[
                f"calculate_sum_zscores_{paramId}_{zscore_sum_rolling_period}"
            ],
            zscore_sum_avg_rolling_period,
        )
        return data
    except Exception as ex:
        logger.exception(traceback)


def changePer_rolling_avg_tagging(strategy_dict: dict, data: pd.DateOffset):
    try:
        # Determine tagging based on rolling average
        def determine_tagging(rolling_avg):
            if pd.isna(rolling_avg):
                return None
            return "GREEN" if rolling_avg > 0 else "RED"

        paramId = strategy_dict["parameterId"]
        rolling_period = strategy_dict["change_per_avg_rolling_period"]
        # Calculate percentage change
        data[
            f"change_pct_{
            paramId}"
        ] = (data[GlobalVariables.OHLC_CLOSE].pct_change() * 100)
        # Calculate rolling average of percentage change
        data[f"change_pct__avg_{paramId}_{rolling_period}"] = (
            data[
                f"change_pct_{
            paramId}"
            ]
            .rolling(window=rolling_period)
            .mean()
        )
        # Determine color based on rolling average
        data[f"change_pct_tagging_{paramId}"] = data[
            f"change_pct__avg_{
            paramId}_{rolling_period}"
        ].apply(determine_tagging)
        return data
    except Exception as ex:
        logging.exception(traceback.format_exc())


def opt_summary(strategy_dict: dict, data: pd.DataFrame):
    try:

        def determine_tagging(rolling_avg, threshold):
            if pd.isna(rolling_avg):
                return None
            return 1 if rolling_avg > threshold else 0

        paramId = strategy_dict["parameterId"]
        rolling_period = strategy_dict["opt_summary_avg_period"]
        threshold = strategy_dict["opt_summary_threshold"]
        symbol = strategy_dict["instrument"]
        file_to_read = f"C:\\OptionChain_Data\\{symbol}.csv"
        if not os.path.exists(file_to_read):
            logger.error(f"Not found IV data {file_to_read}")
            return data
        opt_iv_df = pd.read_csv(file_to_read)
        opt_iv_df["dt"] = pd.to_datetime(opt_iv_df["dt"])
        df = pd.merge(
            data,
            opt_iv_df[["dt", "z_score_call_put_diff_pos"]],
            left_on="e_dt",
            right_on="dt",
            how="left",
        )
        df[
            f"z_score_call_put_diff_pos_avg_{paramId}_{
            rolling_period}"
        ] = (
            df[f"z_score_call_put_diff_pos"]
            .rolling(window=rolling_period)
            .mean()
        )
        df[f"z_score_call_put_diff_pos_tagging_{paramId}_{threshold}"] = df[
            f"z_score_call_put_diff_pos_avg_{
            paramId}_{rolling_period}"
        ].apply(determine_tagging, threshold=threshold)
        return df
    except Exception as ex:
        logging.exception(traceback.format_exc())


def fractal(strategy_dict: dict, data: pd.DataFrame):
    try:
        paramId = strategy_dict["parameterId"]
        fractal_period = strategy_dict["fractal_period"]
        df = Fractal.calculate_fractal(data, fractal_period=fractal_period)
        fractal_high_low_avg_peirod = strategy_dict[
            "fractal_high_low_avg_peirod"
        ]
        fractal_low_threshold = strategy_dict["fractal_low_threshold"]
        fractal_high_threshold = strategy_dict["fractal_high_threshold"]
        # Calculate rolling averages
        window = 4
        df[
            f"rolling_avg_fractal_low_{paramId}_{fractal_high_low_avg_peirod}"
        ] = Fractal.calculate_rolling_average(
            df,
            fractal_high_low_avg_peirod,
            "fractal_low",
            GlobalVariables.FRACTAL_CONFIRMED_LONG,
        )
        df[
            f"rolling_avg_fractal_high{paramId}_{fractal_high_low_avg_peirod}"
        ] = Fractal.calculate_rolling_average(
            df,
            fractal_high_low_avg_peirod,
            "fractal_high",
            GlobalVariables.FRACTAL_CONFIRMED_SHORT,
        )
        df[f"fractal_low_per_{paramId}"] = (
            df[GlobalVariables.OHLC_LOW]
            / df[
                f"rolling_avg_fractal_low_{paramId}_{fractal_high_low_avg_peirod}"
            ]
            - 1
        ) * 100
        df[f"fractal_high_per_{paramId}"] = (
            df[GlobalVariables.OHLC_HIGH]
            / df[
                f"rolling_avg_fractal_high{paramId}_{fractal_high_low_avg_peirod}"
            ]
            - 1
        ) * 100
        df[f"fractal_green_tagging_{paramId}_{fractal_low_threshold}"] = (
            np.where(
                df[f"fractal_low_per_{paramId}"] < fractal_low_threshold,
                1,
                np.nan,
            )
        )
        df[f"fractal_red_tagging_{paramId}_{fractal_high_threshold}"] = (
            np.where(
                df[f"fractal_high_per_{paramId}"] < fractal_high_threshold,
                1,
                np.nan,
            )
        )
        return df
    except Exception as ex:
        logging.exception(ex)


def load_init_data(strategy_df: pd.DataFrame):
    try:
        logger.info(f"loading init data BASE_TIMEFRAME {BASE_TIMEFRAME}")
        obj_readData = readData()
        data_collection = {}
        instrument_unique_dt_collection = {}
        for index, row in strategy_df.iterrows():
            instrument = row["instrument"]
            if instrument in data_collection:
                continue
            json_data = obj_readData.read_spot_data(
                instrument, BT_START_DT, BT_END_DT
            )
            if json_data is None or len(json_data) == 0:
                logger.info(
                    f"insturment : {
                            instrument} loading init data failed"
                )
                continue
            process_data, unique_dt = process_json_data(json_data)
            logger.info(f"instrument {instrument} json data process done")
            tf_start_end_time = creating_tf_start_end_time(
                unique_dt, BASE_TIMEFRAME
            )
            logger.info(
                f"instrument {
                        instrument} calculating start end time for base timframe {BASE_TIMEFRAME}"
            )
            process_data = fill_missing_bars_to_base_timeframe(
                tf_start_end_time, process_data
            )
            logger.info(
                f"instrument {instrument} fill missing time data process done"
            )
            data_collection[instrument] = process_data
            instrument_unique_dt_collection[instrument] = unique_dt
            logger.info(f"instrument {instrument} loading init data done")
            pass
        return data_collection, instrument_unique_dt_collection
        # init_ohlc_calculation(strategy_dict,data_collection,instrument_unique_dt_collection)
    except Exception as ex:
        logger.exception(traceback.format_exc())


def convert_data_to_tf(data, tf, bar_start_end_time):
    try:
        new_data = {}
        for startTime, dts in bar_start_end_time.items():
            new_bar = {
                GlobalVariables.OHLC_DT: None,
                GlobalVariables.OHLC_OPEN: 0,
                GlobalVariables.OHLC_HIGH: 0,
                GlobalVariables.OHLC_LOW: 0,
                GlobalVariables.OHLC_CLOSE: 0,
                GlobalVariables.OHLC_VOLUME: 0,
            }
            for dt in dts:
                ohlc = None
                if dt in data:
                    ohlc = data[dt]
                if not ohlc is None:
                    if new_bar[GlobalVariables.OHLC_OPEN] == 0:
                        new_bar[GlobalVariables.OHLC_DT] = startTime
                        new_bar[GlobalVariables.OHLC_OPEN] = ohlc[
                            GlobalVariables.OHLC_OPEN
                        ]
                        new_bar[GlobalVariables.OHLC_HIGH] = ohlc[
                            GlobalVariables.OHLC_HIGH
                        ]
                        new_bar[GlobalVariables.OHLC_LOW] = ohlc[
                            GlobalVariables.OHLC_LOW
                        ]
                        new_bar[GlobalVariables.OHLC_CLOSE] = ohlc[
                            GlobalVariables.OHLC_CLOSE
                        ]
                        new_bar[GlobalVariables.OHLC_VOLUME] = ohlc[
                            GlobalVariables.OHLC_VOLUME
                        ]
                        new_bar[GlobalVariables.OHLC_END_DT] = dt
                    elif new_bar[GlobalVariables.OHLC_OPEN] > 0:
                        if (
                            ohlc[GlobalVariables.OHLC_HIGH]
                            > new_bar[GlobalVariables.OHLC_HIGH]
                        ):
                            new_bar[GlobalVariables.OHLC_HIGH] = ohlc[
                                GlobalVariables.OHLC_HIGH
                            ]
                        if (
                            ohlc[GlobalVariables.OHLC_LOW]
                            < new_bar[GlobalVariables.OHLC_LOW]
                        ):
                            new_bar[GlobalVariables.OHLC_LOW] = ohlc[
                                GlobalVariables.OHLC_LOW
                            ]
                        new_bar[GlobalVariables.OHLC_CLOSE] = ohlc[
                            GlobalVariables.OHLC_CLOSE
                        ]
                        new_bar[GlobalVariables.OHLC_VOLUME] += ohlc[
                            GlobalVariables.OHLC_VOLUME
                        ]
                        new_bar[GlobalVariables.OHLC_END_DT] = dt
                elif new_bar[GlobalVariables.OHLC_OPEN] > 0:
                    new_bar[GlobalVariables.OHLC_END_DT] = dt
            if new_bar[GlobalVariables.OHLC_DT] is None:
                continue
            new_data[startTime] = new_bar
        return new_data
    except Exception as ex:
        logger.exception(traceback.format_exc())


def init_ohlc_calculation(
    strategy_df: pd.DataFrame,
    data_collection: dict,
    instrument_unique_dt_collection: dict,
):
    try:
        logger.info(f"calculating ohlc from BASE TIMEFRAME {BASE_TIMEFRAME}")
        tf_data_collection = {}
        for index, row in strategy_df.iterrows():
            instrument = row["instrument"]
            tf = row["tf"]
            function = row["function"]
            if not instrument in data_collection:
                logger.info(
                    f"instrument {instrument} base timeframe data not found in data_collection"
                )
                continue
            if not instrument in tf_data_collection:
                tf_data_collection[instrument] = {}
                logger.info(
                    f"instrument {instrument} empty dict added to tf_data_collection"
                )
            instrument_data_collection = tf_data_collection[instrument]
            if not tf in instrument_data_collection:
                unique_dt = instrument_unique_dt_collection[instrument]
                process_data = data_collection[instrument]
                tf_start_end_time = creating_tf_start_end_time(unique_dt, tf)
                logger.info(
                    f"instrument {instrument} calculating start end time for timframe {tf}"
                )
                tf_process_data = convert_data_to_tf(
                    process_data, tf, tf_start_end_time
                )
                logger.info(
                    f"instrument {instrument} calculating  OHLC for timframe {tf}"
                )
                instrument_data_collection[tf] = pd.DataFrame(
                    tf_process_data.values()
                )
                logger.info(
                    f"instrument {instrument} converted  OHLC for timframe {
                            tf} to dataframe"
                )
        return tf_data_collection
        # df = instrument_data_collection[tf]
        # logger.info(f"instrument {instrument} data reterive from instrument_data_collection for TF {tf}")
        # if not function in FUNCTION_DISPATCHER_FOR_FUNCTIONS:
        #     logger.info(f"instrument {instrument} function not found in FUNCTION_DISPATCHER_FOR_FUNCTIONS {function}")
        # instrument_data_collection[tf] = FUNCTION_DISPATCHER_FOR_FUNCTIONS[function](row,df)
    except Exception as ex:
        logger.exception(traceback.format_exc())


def tag_iv_data(df, symbol):
    try:
        file_to_read = f"C:\\OptionChain_Data\\{symbol}.csv"
        if not os.path.exists(file_to_read):
            logger.error(f"Not found IV data {file_to_read}")
            return df
        opt_iv_df = pd.read_csv(file_to_read)
        opt_iv_df["dt"] = pd.to_datetime(opt_iv_df["dt"])
        df = pd.merge(
            df,
            opt_iv_df[
                ["dt", "iv", "hist_vol_30d", "iv_premium", "iv_prem_30d_prev"]
            ],
            left_on="e_dt",
            right_on="dt",
            how="left",
        )
        return df
    except Exception as ex:
        logger.exception(traceback.format_exc())


def init_functions(
    strategy_dict: dict, tf_data_collection, strategy="VOLATILITY"
):
    try:
        strategyId = 0
        instrument = None
        tf = 0
        output_df = []
        for index, row in strategy_dict.items():
            strategyId = row["strategyId"]
            instrument = row["instrument"]
            tf = row["tf"]
            # function = row["function"]
            function = strategy
            paramId = row["parameterId"]
            if not instrument in tf_data_collection:
                logger.info(
                    f"instrument {instrument} data not found in tf_data_collection"
                )
                continue
            instrument_data_collection = tf_data_collection[instrument]
            if not tf in instrument_data_collection:
                logger.info(
                    f"instrument {instrument} tf {
                            tf} data not found in instrument_data_collection"
                )
                continue
            df = instrument_data_collection[tf].copy()
            logger.info(
                f"instrument {
                        instrument} data reterive from instrument_data_collection for TF {tf}"
            )
            if not function in FUNCTION_DISPATCHER_FOR_FUNCTIONS:
                logger.info(
                    f"instrument {
                            instrument} function not found in FUNCTION_DISPATCHER_FOR_FUNCTIONS {function}"
                )
            Fractal.calculate_fractal(df)
            temp_df = FUNCTION_DISPATCHER_FOR_FUNCTIONS[function](row, df)
            logger.info(
                f"instrument {instrument} function calculation for paramId {
                        paramId} done"
            )
            output_df.append(temp_df)
        # Add IV data
        # Append df2 to df1
        df = pd.concat(output_df, axis=1)
        # Drop columns with duplicate names
        df = df.loc[:, ~df.columns.duplicated()]
        # fileName = f"{os.getcwd()}\\Abhishek\\{instrument}_TF_{tf}.csv"
        folderName = f"{os.getcwd()}\\{function}"
        os.makedirs(folderName, exist_ok=True)
        fileName = os.path.join(folderName, f"{instrument}_TF_{tf}.csv")
        df.to_csv(fileName)
        logger.info(
            f"instrument {instrument} logging data for strategy {
                    strategyId} fileName : {fileName}"
        )
    except Exception as ex:
        logger.exception(traceback.format_exc())


def run(strategy_df:pd.DataFrame, symbol: str, strategy: str):
    try:
        if not symbol is None:
            strategy_df["instrument"] = symbol
        data_collection, instrument_unique_dt_collection = load_init_data(
            strategy_df
        )
        tf_data_collection = init_ohlc_calculation(
            strategy_df, data_collection, instrument_unique_dt_collection
        )
        for groupId, group_df in strategy_df.groupby("strategyId"):
            df_dict = group_df.set_index("parameterId").T.to_dict()
            # Adjusting the values to include the key as part of the values
            groupBy_dict = {
                key: {"parameterId": key, **value}
                for key, value in df_dict.items()
            }
            init_functions(groupBy_dict, tf_data_collection, strategy=strategy)
    except Exception as ex:
        logger.exception(traceback.format_exc())


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Run strategy with specified JSON file."
    )
    parser.add_argument(
        "-s",
        "--symbol",
        help="Symbol to run",
        required=False,
        dest="symbol",
        default=None,
    )
    return parser.parse_args()


FUNCTION_DISPATCHER_FOR_FUNCTIONS = {
    "VOLATILITY": volatility,
    "VOLUME": volume,
    "CHANGEPER": changePer_rolling_avg_tagging,
    "OPT_SUMMARY": opt_summary,
    "FRACTAL": fractal,
}


def execute_strategy(symbol, strategy, strategy_df):
    run(strategy_df, symbol, strategy)


def execute_data_processing(total_length, status, error_mssg, datas):
    num_workers = min(
        int(multiprocessing.cpu_count() * 0.8),
        total_length,
    )
    results = []
    batch_size = 20
    with multiprocessing.Pool(num_workers) as pool:
        for i in range(0, total_length, batch_size):
            batch = datas[i : i + batch_size]  # Create a batch of 10 items
            results = []

            # Submit tasks for the current batch
            for data in batch:
                result = pool.apply_async(run, kwds=data)
                results.append(result)

            # Wait for all tasks in the current batch to complete
            for result in results:
                try:
                    result.get()  # This will raise an exception if the process failed
                    status.append("SUCCESS")
                    error_mssg.append("")
                except Exception as e:
                    logger.error(f"Error processing volatile data: {e}")
                    status.append("ERROR")
                    error_mssg.append(str(e))


def multiple_strategy_execution(input_data, volatile_df, volume_df):
    datas, status, error_message = [], [], []
    for instrument in input_data["instrument"]:
        for strategy in input_data["strategies"]:
            if strategy == Strategy.VOLATILITY:
                datas.append(
                    {
                        "strategy_df": volatile_df,
                        "symbol": instrument,
                        "strategy": strategy.value,
                    }
                )
            elif strategy == Strategy.VOLUME:
                datas.append(
                    {
                        "strategy_df": volume_df,
                        "symbol": instrument,
                        "strategy": strategy.value,
                    }
                )

    execute_data_processing(
        len(datas),
        status=status,
        error_mssg=error_message,
        datas=datas,
    )

    status_df = pd.DataFrame(datas)
    status_df["status"] = status
    status_df["error_message"] = error_message

    now_str = pd.Timestamp.now().strftime("%Y-%m-%d %H-%M-%S")
    write_dataframe_to_csv(
        status_df,
        "OUTPUT_STATUS",
        f"{now_str}_output.csv",
    )
