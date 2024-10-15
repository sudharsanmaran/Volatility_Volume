from enum import Enum
import logging
from typing import Tuple
import pandas as pd
from pydantic import BaseModel, field_validator

logger = logging.getLogger(__name__)


class Strategy(Enum):
    VOLATILITY = "VOLATILITY"
    VOLUME = "VOLUME"


class Inputs(BaseModel):
    strategies: list[Strategy]
    instrument: str

    @field_validator("instrument", mode="after")
    def validate_instrument(cls, value):
        return tuple(value.split(","))


def validate_inputs(inputs: dict):
    try:
        return Inputs(**inputs).model_dump()
    except Exception as e:
        logger.error(f"Error validating inputs: {e}")
        raise e


def validate_file(file):
    required_columns = {
        "strategyId",
        "parameterId",
        "tf",
        "stdv_rolling_period",
        "stdv_neg",
        "avg_volatility_rolling_peirod",
        "zscore_sum_rolling_period",
        "zscore_sum_avg_rolling_period",
        "change_per_avg_rolling_period",
        "opt_summary_avg_period",
        "opt_summary_threshold",
        "fractal_period",
        "fractal_high_low_avg_peirod",
        "fractal_low_threshold",
        "fractal_high_threshold",
    }

    try:
        df = pd.read_csv(file)
        if missing := required_columns - set(df.columns):
            raise ValueError(
                f"Missing required columns in selected file: {missing}"
            )
        type_cast_dataframe(df)
        return df
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        raise e


def type_cast_dataframe(df):
    df["strategyId"] = df["strategyId"].astype(int)
    df["parameterId"] = df["parameterId"].astype(int)
    df["tf"] = df["tf"].astype(int)
    df["stdv_rolling_period"] = df["stdv_rolling_period"].astype(int)
    df["stdv_neg"] = df["stdv_neg"].astype(bool)
    df["avg_volatility_rolling_peirod"] = df[
        "avg_volatility_rolling_peirod"
    ].astype(int)
    df["zscore_sum_rolling_period"] = df["zscore_sum_rolling_period"].astype(
        int
    )
    df["zscore_sum_avg_rolling_period"] = df[
        "zscore_sum_avg_rolling_period"
    ].astype(int)
    df["change_per_avg_rolling_period"] = df[
        "change_per_avg_rolling_period"
    ].astype(int)
    df["opt_summary_avg_period"] = df["opt_summary_avg_period"].astype(int)
    df["opt_summary_threshold"] = df["opt_summary_threshold"].astype(int)
    df["fractal_period"] = df["fractal_period"].astype(int)
    df["fractal_high_low_avg_peirod"] = df[
        "fractal_high_low_avg_peirod"
    ].astype(int)
    df["fractal_low_threshold"] = df["fractal_low_threshold"].astype(int)
    df["fractal_high_threshold"] = df["fractal_high_threshold"].astype(int)
