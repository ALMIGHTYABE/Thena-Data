import requests
import pandas as pd
import numpy as np
import yaml
import json
import os
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta, TH
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

try:
    # Params Data
    subgraph = config["query"]["subgraph"]
    id_data = config["files"]["id_data"]
    provider_url = config["web3"]["provider_url"]
    bribe_abi = config["web3"]["bribe_abi"]
    epoch_csv = config["files"]["epoch_data"]
    price_api = config["api"]["price_api"]
    fee_csv = config["files"]["fee_data"]

    # Pulling Bribe Data
    logger.info("Fee Data Started")

    ids_df = pd.read_csv(id_data)

    # Get Epoch Timestamp
    todayDate = datetime.utcnow()
    if todayDate.isoweekday() == 4:
        nextThursday = todayDate + relativedelta(weekday=TH(2))
        my_time = datetime.min.time()
        my_datetime = datetime.combine(nextThursday, my_time)
        timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
        print("Yes, The next Thursday date:", my_datetime, timestamp)
    else:
        nextThursday = todayDate + relativedelta(weekday=TH(0))
        my_time = datetime.min.time()
        my_datetime = datetime.combine(nextThursday, my_time)
        timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
        print("No, The next Thursday date:", my_datetime, timestamp)

    # Read Epoch Data
    epoch_data = pd.read_csv(epoch_csv)
    epoch = epoch_data[epoch_data["timestamp"] == timestamp]["epoch"].values[0] - 1

    # Pull Fees Web3
    validation.METHODS_TO_VALIDATE = []
    w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 60}))

    fees_list = []
    for name, Fee_ca in zip(ids_df["name"], ids_df["fee_ca"]):
        if fee_ca == "0x0000000000000000000000000000000000000000":
            pass
        else:
            contract_address = fee_ca
            contract_instance = w3.eth.contract(address=contract_address, abi=bribe_abi)

            rewardsListLength = contract_instance.functions.rewardsListLength().call()

            rewardTokens = []
            for reward_num in range(rewardsListLength):
                rewardTokens.append(contract_instance.functions.rewardTokens(reward_num).call())

            for reward_addy in rewardTokens:
                rewarddata = contract_instance.functions.rewardData(reward_addy, timestamp).call()
                if rewarddata[1] > 0:
                    fees_list.append({"name": name, "fees": rewarddata[1], "address": reward_addy})

    fee_df = pd.DataFrame(fees_list)
    fee_df["address"] = fee_df["address"].apply(str.lower)

    # Pull Prices
    response = requests.get(price_api)
    pricelist = []
    for i in response.json()["data"]:
        pricelist.append([i["name"], i["address"], i["price"], i["decimals"]])

    price_df = pd.DataFrame(pricelist, columns=["name", "address", "price", "decimals"])

    # Fee Amounts
    fee_df = fee_df.merge(price_df[["address", "price", "decimals"]], on="address", how="left")
    fee_df["fee_amount"] = fee_df["price"] * fee_df["fees"]

    fee_amount = []
    for dec, amt in zip(fee_df["decimals"], fee_df["fee_amount"]):
        decimal = "1"
        decimal = decimal.ljust(dec + 1, "0")
        fee_amount.append((amt / int(decimal)))

    fee_df["fee_amount"] = fee_amount

    print(fee_df)
    fee_df = fee_df.groupby(by="name")["fee_amount"].sum().reset_index()
    fee_df["epoch"] = epoch
    print(fee_df)

    # Rewriting current Epoch's Fee Data
    feeor = pd.read_csv(fee_csv)
    current_fee_index = feeor[feeor["epoch"] == epoch].index
    feeor.drop(current_fee_index, inplace=True)
    fee_df = pd.concat([feeor, fee_df], ignore_index=True, axis=0)

    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["fee_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    worksheet1.clear()
    set_with_dataframe(
        worksheet=worksheet1,
        dataframe=fee_df,
        include_index=False,
        include_column_header=True,
        resize=True,
    )

    logger.info("FEE Data Ended")
except Exception as e:
    logger.error("Error occurred during FEE Data process. Error: %s" % e)
