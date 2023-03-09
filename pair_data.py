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
import itertools


# Params
params_path = "params.yaml"


def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


config = read_params(params_path)

try:
    # Pulling IDs
    logger.info("ID Data Started")

    # Params Data
    subgraph = config["query"]["subgraph"]
    myobj1 = config["query"]["id_data_query"]
    myobj2 = config["query"]["pair_data_query"]
    epoch_daily_csv = config["files"]["epoch_daily_data"]

    # Request
    ids_df = pd.DataFrame()
    for i in itertools.count(0, 100):
        myobj1["variables"]["skip"] = i
        response = requests.post(url=subgraph, json=myobj1)
        data = response.json()["data"]["pairs"]

        # Checking if empty data
        if data == []:
            break
        else:
            temp_df = pd.json_normalize(data)
            ids_df = pd.concat([ids_df, temp_df], axis=0)
    ids_df.reset_index(drop=True, inplace=True)

    # Web3
    provider_url = config["web3"]["provider_url"]
    w3 = Web3(Web3.HTTPProvider(provider_url))
    amm_abi = config["web3"]["amm_abi"]

    names = []
    for address in ids_df["id"]:
        address = w3.toChecksumAddress(address)
        contract_instance = w3.eth.contract(address=address, abi=amm_abi)
        names.append({"name": contract_instance.functions.symbol().call(), "address": address})

    ids_df = pd.DataFrame(names)
    ids_df[["type", "pair"]] = ids_df["name"].str.split("-", 1, expand=True)
    ids_df.drop(["pair"], axis=1, inplace=True)

    logger.info("ID Data Ended")

    # Pulling Pair Data
    logger.info("Pair Data Started")

    # Request and Edit Pair Data
    pairdata_df = pd.DataFrame()

    for name, contract_address in zip(ids_df["name"], ids_df["address"]):
        try:
            myobj2["variables"]["pairAddress"] = contract_address
            for i in itertools.count(0, 100):
                myobj2["variables"]["skip"] = i
                response = requests.post(subgraph, json=myobj2)
                data = response.json()["data"]["pairDayDatas"]

                # Checking if empty data
                if data == []:
                    break
                else:
                    df = pd.json_normalize(data)
                    df["name"] = name
                    drop_index = df[df["date"].astype("str") == "1672790400"].index
                    df.drop(drop_index, inplace=True)
                    pairdata_df = pd.concat([pairdata_df, df], axis=0, ignore_index=True)
            pairdata_df.reset_index(drop=True, inplace=True)
        except Exception as e:
            logger.error("Error occurred during Pair Data process. Pair: %s, Address: %s, Error: %s" % (name, contract_address, e))

    epoch_data = pd.read_csv(epoch_daily_csv)
    epoch_data["date"] = epoch_data["date"].apply(lambda date: datetime.strptime(date, "%d-%m-%Y").date())

    pairdata_df["date"] = pairdata_df["date"].apply(lambda timestamp: datetime.utcfromtimestamp(timestamp).date())
    pairdata_df = pd.merge(pairdata_df, ids_df, how="left", on="name")
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
