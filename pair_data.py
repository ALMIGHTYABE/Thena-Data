import requests
import pandas as pd
import yaml
import json
import os
from datetime import datetime, timezone, date, timedelta
from application_logging.logger import logger
import gspread
from gspread_dataframe import set_with_dataframe
from web3 import Web3
from web3.middleware import validation


# Params
params_path = "params.yaml"


def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


config = read_params(params_path)

# V1
try:
    # Params Data
    subgraph = config["query"]["subgraph"]
    id_data = config["files"]["id_data"]
    pair_data_query = config["query"]["pair_data_query"]
    epoch_daily_csv = config["files"]["epoch_daily_data"]
    pair_data_csv = config["files"]["pair_data"]
    provider_url = config["web3"]["provider_url"]

    # Pulling Pair Data
    logger.info("Pair Data Started")

    # Request and Edit Pair Data
    ids_df = pd.read_csv(id_data)
    ids_df = ids_df[ids_df['type'] != "CL"]
    
    # Today and 2 Day Ago
    todayDate = datetime.utcnow()
    twodayago = todayDate - timedelta(2)
    my_time = datetime.min.time()
    my_datetime = datetime.combine(twodayago, my_time)
    timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())

    # Web3
    validation.METHODS_TO_VALIDATE = []
    w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 60}))

    pairdata_df = pd.DataFrame()
    for name, contract_address in zip(ids_df["name"], ids_df["address"]):
        try:
            pair_data_query["variables"]["pairAddress"] = contract_address
            pair_data_query["variables"]["startTime"] = timestamp
            response = requests.post(subgraph, json=pair_data_query, timeout=60)
            data = response.json()["data"]["pairDayDatas"]
            df = pd.json_normalize(data)
            df["name"] = name
            pairdata_df = pd.concat([pairdata_df, df], axis=0, ignore_index=True)
            pairdata_df.reset_index(drop=True, inplace=True)
        except Exception as e:
            logger.error("Error occurred during Pair Data process. Pair: %s, Address: %s, Error: %s" % (name, contract_address, e))

    epoch_data = pd.read_csv(epoch_daily_csv)
    epoch_data["date"] = epoch_data["date"].apply(lambda date: datetime.strptime(date, "%d-%m-%Y").date())

    pairdata_df["date"] = pairdata_df["date"].apply(lambda timestamp: datetime.utcfromtimestamp(timestamp).date())
    pairdata_df = pd.merge(pairdata_df, ids_df[["name", "address", "type"]], how="left", on="name")
    pairdata_df = pd.merge(pairdata_df, epoch_data[["date", "epoch"]], how="left", on="date")

    pairdata_df["fee %"] = pairdata_df["type"]
    pairdata_df["fee %"].replace({"vAMM": 0.20, "sAMM": 0.01}, inplace=True)

    pairdata_df["dailyVolumeUSD"] = pd.to_numeric(pairdata_df["dailyVolumeUSD"])
    pairdata_df["fee"] = (pairdata_df["dailyVolumeUSD"] * pairdata_df["fee %"]) / 100
    pairdata_df.sort_values("date", ascending=True, inplace=True)
    pairdata_df["date"] = pairdata_df["date"].apply(lambda date: datetime.strftime(date, "%Y-%m-%d"))
    pairdata_df = pairdata_df[['id', 'date', 'dailyVolumeToken0', 'dailyVolumeToken1', 'dailyVolumeUSD', 'reserveUSD', '__typename', 'name', 'address', 'type', 'epoch', 'fee %', 'fee']]
    
    pairdata_old = pd.read_csv(pair_data_csv)
    drop_index = pairdata_old[pairdata_old['date']>datetime.fromtimestamp(timestamp).strftime(format='%Y-%m-%d')].index
    index_list = drop_index.to_list()
    index_list = list(map(lambda x: x + 2, index_list))
    pairdata_df['__typename'] = 'V1'
    df_values = pairdata_df.values.tolist()

    if pairdata_df.empty:
        raise Exception("Dataframe is empty")

    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["pair_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    if index_list != []:
        worksheet1.delete_rows(index_list[0], index_list[-1])

    # Append to Worksheet
    gs.values_append("Master", {"valueInputOption": "USER_ENTERED"}, {"values": df_values})

    logger.info("Pair Data Ended")
except Exception as e:
    logger.error("Error occurred during Pair Data process. Error: %s" % e, exc_info=True)
    
   
# Fusion
try:
    # Params Data
    subgraph = config["query"]["fusion_subgraph"]
    id_data = config["files"]["id_data"]
    pair_data_fusion_query = config["query"]["pair_data_fusion_query"]
    epoch_daily_csv = config["files"]["epoch_daily_data"]
    pair_data_fusion_csv = config["files"]["pair_data_fusion"]
    provider_url = config["web3"]["provider_url"]

    # Pulling Pair Data
    logger.info("Pair Data Fusion Started")

    # Request and Edit Pair Data
    ids_df = pd.read_csv(id_data)
    ids_df = ids_df[ids_df["type"]=="CL"].copy(deep=True)
    ids_df = ids_df[['name', 'algebra_pool', 'type']]
    ids_df.sort_values('name', ascending=False, inplace=True)
    ids_df.drop_duplicates(subset=['algebra_pool'], keep='first', inplace=True)
    ids_df.loc[ids_df['name'].str.startswith('a'), 'name'] = ids_df.loc[ids_df['name'].str.startswith('a'), 'name'].str[:-5]
    
    # Today and 2 Day Ago
    todayDate = datetime.utcnow()
    twodayago = todayDate - timedelta(2)
    my_time = datetime.min.time()
    my_datetime = datetime.combine(twodayago, my_time)
    timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())

    # Web3
    validation.METHODS_TO_VALIDATE = []
    w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 60}))

    pairdata_fusion_df = pd.DataFrame()
    for name, contract_address in zip(ids_df["name"], ids_df["algebra_pool"]):
        try:
            pair_data_fusion_query["variables"]["pairAddress"] = contract_address.lower()
            pair_data_fusion_query["variables"]["startTime"] = timestamp
            response = requests.post(subgraph, json=pair_data_fusion_query, timeout=60)
            data = response.json()["data"]["poolDayDatas"]
            df = pd.json_normalize(data)
            df["name"] = name
            pairdata_fusion_df = pd.concat([pairdata_fusion_df, df], axis=0, ignore_index=True)
            pairdata_fusion_df.reset_index(drop=True, inplace=True)
        except Exception as e:
            logger.error("Error occurred during Pair Data Fusion process. Pair: %s, Address: %s, Error: %s" % (name, contract_address, e))

    epoch_data = pd.read_csv(epoch_daily_csv)
    epoch_data["date"] = epoch_data["date"].apply(lambda date: datetime.strptime(date, "%d-%m-%Y").date())

    pairdata_fusion_df["date"] = pairdata_fusion_df["date"].apply(lambda timestamp: datetime.utcfromtimestamp(timestamp).date())
    pairdata_fusion_df = pd.merge(pairdata_fusion_df, ids_df[["name", "algebra_pool", "type"]], how="left", on="name")
    pairdata_fusion_df = pd.merge(pairdata_fusion_df, epoch_data[["date", "epoch"]], how="left", on="date")
    pairdata_fusion_df.sort_values("date", ascending=True, inplace=True)
    pairdata_fusion_df["date"] = pairdata_fusion_df["date"].apply(lambda date: datetime.strftime(date, "%Y-%m-%d"))
    
    pairdata_fusion_old = pd.read_csv(pair_data_fusion_csv)
    drop_index = pairdata_fusion_old[pairdata_fusion_old['date']>datetime.fromtimestamp(timestamp).strftime(format='%Y-%m-%d')].index
    index_list = drop_index.to_list()
    index_list = list(map(lambda x: x + 2, index_list))
    pairdata_fusion_df['__typename'] = 'Fusion'
    df_values = pairdata_fusion_df.values.tolist()

    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["pair_data_fusion_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    if index_list != []:
        worksheet1.delete_rows(index_list[0], index_list[-1])

    # Append to Worksheet
    gs.values_append("Master", {"valueInputOption": "USER_ENTERED"}, {"values": df_values})

    logger.info("Pair Data Fusion Ended")
except Exception as e:
    logger.error("Error occurred during Pair Data Fusion process. Error: %s" % e, exc_info=True)
    
    
    # Combined
try:
    logger.info("Pair Data Combined Started")

    # Data Manipulation
    df1 = pd.read_csv(pair_data_csv)
    df2 = pd.read_csv(pair_data_fusion_csv)
    df2['fee %'] = 0
    df2 = df2[['id', 'date', 'volumeToken0', 'volumeToken1', 'volumeUSD', 'tvlUSD', '__typename', 'name', 'algebra_pool', 'type',  'epoch', 'fee %', 'feesUSD']]
    df2.columns = ['id', 'date', 'dailyVolumeToken0', 'dailyVolumeToken1', 'dailyVolumeUSD', 'reserveUSD', '__typename', 'name', 'address', 'type', 'epoch', 'fee %', 'fee']
    pairdata_combined_df = pd.concat([df1, df2], ignore_index=True, axis=0)

    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["pair_data_combined_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    worksheet1.clear()
    set_with_dataframe(
        worksheet=worksheet1,
        dataframe=pairdata_combined_df,
        include_index=False,
        include_column_header=True,
        resize=True,
    )

    logger.info("Pair Data Combined Ended")
except Exception as e:
    logger.error("Error occurred during Pair Data Combined process. Error: %s" % e, exc_info=True)

