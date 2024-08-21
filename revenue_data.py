import pandas as pd
import numpy as np
import json
import os
from application_logging.logger import logger
import gspread
from gspread_dataframe import set_with_dataframe
from utils.helpers import read_params

# Params
params_path = 'params.yaml'
config = read_params(params_path)

try:
    logger.info("Revenue Data Started")

    # Params Data
    pair_data = config["files"]["pair_data_combined"]
    bribe_data = config["files"]["bribe_data"]
    emissions_data = config["files"]["emissions_data"]

    # Read Data
    pair_df = pd.read_csv(pair_data)
    bribe_df = pd.read_csv(bribe_data)
    emissions_df = pd.read_csv(emissions_data)

    # Data Wrangling
    epoch_wise_fees = pair_df.groupby(["epoch", "name"], as_index=False)["fee"].sum()
    df = pd.merge(epoch_wise_fees, bribe_df, on=["epoch", "name"], how="outer")
    df.replace(np.nan, 0, inplace=True)
    df["revenue"] = df["fee"] + df["bribe_amount"]
    bribe_df_offset = bribe_df.copy(deep=True)
    bribe_df_offset["epoch"] = bribe_df_offset["epoch"] + 1
    bribe_df_offset.columns = ["name", "bribe_amount_offset", "epoch"]
    df = pd.merge(df, bribe_df_offset, on=["epoch", "name"], how="outer")
    final_df = pd.merge(df, emissions_df, on=["epoch", "name"], how="outer")
    final_df.replace(np.nan, 0, inplace=True)
    final_df.sort_values(by="epoch", axis=0, ignore_index=True, inplace=True)
    latest_epoch = final_df["epoch"].iloc[-1]
    latest_data_index = final_df[final_df["epoch"] == latest_epoch].index
    final_df.drop(latest_data_index, inplace=True)

    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["revenue_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    worksheet1.clear()
    set_with_dataframe(
        worksheet=worksheet1,
        dataframe=final_df,
        include_index=False,
        include_column_header=True,
        resize=True,
    )

    logger.info("Revenue Data Ended")
except Exception as e:
    logger.error("Error occurred during Revenue Data process. Error: %s" % e)
