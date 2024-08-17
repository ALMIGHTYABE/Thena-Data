import pandas as pd
import json
import os
from datetime import datetime
from application_logging.logger import logger
import gspread
from utils.helpers import read_params

# Params
params_path = 'params.yaml'
config = read_params(params_path)

try:
    # Params Data
    epoch_daily_csv = config["files"]["epoch_daily_data"]
    pair_data = config["files"]["pair_data_combined"]
    fee_tvl_data_csv = config["files"]["fee_tvl_data"]

    # Epoch we are in
    todayDate = datetime.utcnow().date()
    epoch_data = pd.read_csv(epoch_daily_csv)
    epoch_data["date"] = epoch_data["date"].apply(lambda date: datetime.strptime(date, "%d-%m-%Y").date())
    current_epoch = epoch_data[epoch_data["date"] <= todayDate]["epoch"].max()

    # Pulling Fee TVL Data
    logger.info("Fee TVL Data Started")

    pair_df = pd.read_csv(pair_data)
    if pair_df.empty:
        raise Exception("Dataframe is empty")
    grouped_df = pair_df.groupby('epoch')[['fee', 'reserveUSD']].sum()
    grouped_df['Average TVL'] = grouped_df['reserveUSD'] / 7
    grouped_df = grouped_df.drop(columns='reserveUSD')
    grouped_df['Fee/TVL'] = grouped_df['fee']/grouped_df['Average TVL']
    grouped_df.reset_index(inplace=True)
    grouped_df.sort_values("epoch", ascending=True, inplace=True)
    grouped_df = grouped_df[grouped_df['epoch']>=current_epoch-2]

    fee_tvl_data_old = pd.read_csv(fee_tvl_data_csv)
    drop_index = fee_tvl_data_old[fee_tvl_data_old['epoch']>=current_epoch-2].index
    index_list = drop_index.to_list()
    index_list = list(map(lambda x: x + 2, index_list))
    df_values = grouped_df.values.tolist()

    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["fee_tvl_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    if index_list != []:
        worksheet1.delete_rows(index_list[0], index_list[-1])

    # Append to Worksheet
    gs.values_append("Master", {"valueInputOption": "USER_ENTERED"}, {"values": df_values})

    logger.info("Fee TVL Data Ended")
except Exception as e:
    logger.error("Error occurred during TVL Data process. Error: %s" % e, exc_info=True)