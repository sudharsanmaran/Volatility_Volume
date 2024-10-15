import streamlit as st
from run import multiple_strategy_execution
from validations import validate_file, validate_inputs


def main():

    strategies = st.multiselect(
        options=["VOLATILITY", "VOLUME"],
        label="Select Strategies",
    )

    volatility_file = None
    if "VOLATILITY" in strategies:
        volatility_file = st.file_uploader(
            label="Upload Volatility File",
            type=["csv"],
            accept_multiple_files=False,
        )

    volume_file = None
    if "VOLUME" in strategies:
        volume_file = st.file_uploader(
            label="Upload Volume File",
            type=["csv"],
            accept_multiple_files=False,
        )

    instrument = st.text_input(
        label="Instrument",
        value="REALIANCE, NIFTY",
    )
    required = [instrument, strategies]
    if all(required) and st.button("Run Strategy"):
        if "VOLATILITY" in strategies and volatility_file is None:
            st.error("Please upload Volatility file")
        elif "VOLUME" in strategies and volume_file is None:
            st.error("Please upload Volume file")
        else:
            try:
                validated_inputs = validate_inputs(
                    {
                        "strategies": strategies,
                        "instrument": instrument,
                    }
                )
                volatile_df, volume_df = None, None
                if "VOLATILITY" in strategies:
                    volatile_df = validate_file(volatility_file)
                if "VOLUME" in strategies:
                    volume_df = validate_file(volume_file)
                multiple_strategy_execution(
                    validated_inputs,
                    volatile_df,
                    volume_df,
                )
                st.success("Strategy Executed Successfully")
            except Exception as e:
                st.error(f"Error validating inputs: {e}")


if __name__ == "__main__":
    main()
