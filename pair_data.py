import requests
import pandas as pd
import yaml
import json
import os
from datetime import datetime, date
from application_logging.logger import logger
import gspread
from gspread_dataframe import set_with_dataframe
from web3 import Web3
from web3.middleware import validation
import itertools


# Params
params_path = "params.yaml"


def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


config = read_params(params_path)

try:
    # Params Data
    subgraph = config["query"]["subgraph"]
    id_data = config["files"]["id_data"]
    pair_data_query = config["query"]["pair_data_query"]
    epoch_daily_csv = config["files"]["epoch_daily_data"]
    provider_url = config["web3"]["provider_url"]

    # Pulling Pair Data
    logger.info("Pair Data Started")

    # Request and Edit Pair Data
    ids_df = pd.read_csv(id_data)

    # Web3
    validation.METHODS_TO_VALIDATE = []
    w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 60}))

    pairdata_df = pd.DataFrame()
    for name, contract_address in zip(ids_df["name"], ids_df["address"]):
        try:
            pair_data_query["variables"]["pairAddress"] = contract_address
            for i in itertools.count(0, 100):
                pair_data_query["variables"]["skip"] = i
                response = requests.post(subgraph, json=pair_data_query)
                data = response.json()["data"]["pairDayDatas"]

                # Checking if empty data
                if data == []:
                    break
                else:
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

    edit_index_1 = pairdata_df[(pairdata_df["type"] == "sAMM") & (pairdata_df["date"] > date(2023, 1, 1)) & (pairdata_df["date"] < date(2023, 1, 19))].index
    edit_index_2 = pairdata_df[(pairdata_df["type"] == "sAMM") & (pairdata_df["date"] >= date(2023, 1, 19)) & (pairdata_df["date"] < date(2023, 1, 23))].index
    edit_index_3 = pairdata_df[(pairdata_df["type"] == "sAMM") & (pairdata_df["date"] == date(2023, 1, 23))].index
    pairdata_df.loc[edit_index_1, "fee %"] = 0.04
    pairdata_df.loc[edit_index_2, "fee %"] = 0.03
    pairdata_df.loc[edit_index_3, "fee %"] = 0.02

    pairdata_df["dailyVolumeUSD"] = pd.to_numeric(pairdata_df["dailyVolumeUSD"])
    pairdata_df["fee"] = (pairdata_df["dailyVolumeUSD"] * pairdata_df["fee %"]) / 100

    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["pair_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    worksheet1.clear()
    set_with_dataframe(
        worksheet=worksheet1,
        dataframe=pairdata_df,
        include_index=False,
        include_column_header=True,
        resize=True,
    )

    logger.info("Pair Data Ended")
except Exception as e:
    logger.error("Error occurred during Pair Data process. Error: %s" % e)
