import requests
import pandas as pd
import yaml
import json
import os
from datetime import datetime, timezone, date, timedelta
from application_logging.logger import logger
import gspread
from gspread_dataframe import set_with_dataframe
import itertools


# Params
params_path = "params.yaml"


def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


config = read_params(params_path)

try:
    logger.info("Day Data Started")

    # Params Data
    subgraph = config["query"]["subgraph"]
    day_data_query = config["query"]["day_data_query"]
    daily_data_csv = config["files"]["daily_data"]
    
    # Today and 2 Day Ago
    todayDate = datetime.utcnow()
    twodayago = todayDate - timedelta(2)
    my_time = datetime.min.time()
    my_datetime = datetime.combine(twodayago, my_time)
    timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
    
    # Request
    day_data_df = pd.DataFrame()
    day_data_query["variables"]["startTime"] = timestamp
    response = requests.post(url=subgraph, json=day_data_query)
    data = response.json()["data"]["dayDatas"]
    day_data_df.reset_index(drop=True, inplace=True)
    day_data_df["date"] = day_data_df["date"].apply(lambda timestamp: datetime.utcfromtimestamp(timestamp).date())
    
    day_data_old = pd.read_csv(daily_data_csv)
    drop_index = day_data_old[day_data_old['date']>=datetime.fromtimestamp(timestamp).strftime(format='%Y-%m-%d')].index
    day_data_old.drop(drop_index, inplace=True)
    day_data_df = pd.concat([day_data_old, day_data_df], ignore_index=True, axis=0)
    
    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["daily_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    worksheet1.clear()
    set_with_dataframe(
        worksheet=worksheet1,
        dataframe=day_data_df,
        include_index=False,
        include_column_header=True,
        resize=True,
    )

    logger.info("Day Data Ended")
except Exception as e:
    logger.error("Error occurred during Day Data process. Error: %s" % e)
