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
daydelta = config['delta']['day_data']

try:
    logger.info("Revenue Data Started")

    # Params Data
    id_data = config['files']['id_data']
    pair_data = config["files"]["pair_data_combined"]
    bribe_data = config["files"]["bribe_data"]
    emissions_data = config["files"]["emissions_data"]

    # Read Data
    ids_df = pd.read_csv(id_data)
    pair_df = pd.read_csv(pair_data)
    bribe_df = pd.read_csv(bribe_data)
    emissions_df = pd.read_csv(emissions_data)

    # Data Wrangling
    bribe_df = bribe_df.merge(ids_df[['name', 'new_name', 'alm_type']], how='left', on='name')
    bribe_df['algebra_name'] = np.where(~bribe_df['alm_type'].isin(['vAMM', 'sAMM']), bribe_df['new_name'] + " " + bribe_df['alm_type'], bribe_df['new_name']) # calling it algb name temporarily
    bribe_df = bribe_df[['epoch', 'algebra_name', 'bribe_amount']]
    emissions_df = emissions_df.merge(ids_df[['name', 'new_name', 'alm_type']], how='left', on='name')
    emissions_df['algebra_name'] = np.where(~emissions_df['alm_type'].isin(['vAMM', 'sAMM']), emissions_df['new_name'] + " " + emissions_df['alm_type'], emissions_df['new_name']) # calling it algb name temporarily
    emissions_df = emissions_df[['epoch', 'algebra_name', 'voteweight', 'emissions', 'value', 'THE_price']]
    epoch_wise_fees = pair_df.groupby(["epoch", "algebra_name"], as_index=False)["fee"].sum()
    df = pd.merge(epoch_wise_fees, bribe_df, on=["epoch", "algebra_name"], how="outer")
    df.replace(np.nan, 0, inplace=True)
    df["revenue"] = df["fee"] + df["bribe_amount"]
    bribe_df_offset = bribe_df.copy(deep=True)
    bribe_df_offset["epoch"] = bribe_df_offset["epoch"] + 1
    bribe_df_offset.columns = ['epoch', 'algebra_name', 'bribe_amount_offset']
    df = pd.merge(df, bribe_df_offset, on=["epoch", "algebra_name"], how="outer")
    final_df = pd.merge(df, emissions_df, on=["epoch", "algebra_name"], how="outer")
    final_df.replace(np.nan, 0, inplace=True)
    def extract_algebra_name(algebra_name):
        if algebra_name.startswith(('vAMM', 'sAMM')):
            return algebra_name
        return algebra_name.split(' ')[0]
    final_df['final_name'] = final_df['algebra_name'].apply(extract_algebra_name)
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
