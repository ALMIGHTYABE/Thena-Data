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
    provider_urls = config["web3"]["provider_urls"]
    bribe_abi = config["web3"]["bribe_abi"]
    epoch_csv = config["files"]["epoch_data"]
    price_api = config["api"]["price_api"]
    bribe_csv = config["files"]["bribe_data"]

    # Pulling Bribe Data
    logger.info("Bribe Data Started")

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

    # Pull Bribes Web3
    for rpc_endpoint in provider_urls:
        try:
            validation.METHODS_TO_VALIDATE = []
            w3 = Web3(Web3.HTTPProvider(rpc_endpoint, request_kwargs={"timeout": 60}))
        
            bribes_list = []
            for name, bribe_ca in zip(ids_df["name"], ids_df["bribe_ca"]):
                if bribe_ca == "0x0000000000000000000000000000000000000000":
                    pass
                else:
                    contract_address = bribe_ca
                    contract_instance = w3.eth.contract(address=contract_address, abi=bribe_abi)
        
                    rewardsListLength = contract_instance.functions.rewardsListLength().call()
        
                    rewardTokens = []
                    for reward_num in range(rewardsListLength):
                        rewardTokens.append(contract_instance.functions.rewardTokens(reward_num).call())
        
                    for reward_addy in rewardTokens:
                        rewarddata = contract_instance.functions.rewardData(reward_addy, timestamp).call()
                        if rewarddata[1] > 0:
                            bribes_list.append({"name": name, "bribes": rewarddata[1], "address": reward_addy})
        except Exception as e:
            print(f"Error occurred while fetching bribes from {rpc_endpoint}: {e}")

    bribe_df = pd.DataFrame(bribes_list)
    if bribe_df.empty:
        raise Exception("Dataframe is empty")
    
    bribe_df["address"] = bribe_df["address"].apply(str.lower)

    # Pull Prices
    response = requests.get(price_api)
    pricelist = []
    for i in response.json()["data"]:
        pricelist.append([i["name"], i["address"], i["price"], i["decimals"]])

    price_df = pd.DataFrame(pricelist, columns=["name", "address", "price", "decimals"])

    # Bribe Amounts
    bribe_df = bribe_df.merge(price_df[["address", "price", "decimals"]], on="address", how="left")
    bribe_df["bribe_amount"] = bribe_df["price"] * bribe_df["bribes"]

    bribe_amount = []
    for dec, amt in zip(bribe_df["decimals"], bribe_df["bribe_amount"]):
        decimal = "1"
        decimal = decimal.ljust(dec + 1, "0")
        bribe_amount.append((amt / int(decimal)))

    bribe_df["bribe_amount"] = bribe_amount

    # bribe_df["bribe_amount"] = np.where((bribe_df["address"] != "0xe80772eaf6e2e18b651f160bc9158b2a5cafca65") | (bribe_df["address"] != "0x71be881e9c5d4465b3fff61e89c6f3651e69b5bb"), bribe_df["bribe_amount"] / 1000000000000000000, bribe_df["bribe_amount"])

    # four_decimal_index = bribe_df[(bribe_df["address"] == "0x71be881e9c5d4465b3fff61e89c6f3651e69b5bb")].index
    # bribe_df.loc[four_decimal_index, "bribe_amount"] = bribe_df.loc[four_decimal_index, "bribes"] * bribe_df.loc[four_decimal_index, "price"] / 10000

    # six_decimal_index = bribe_df[(bribe_df["address"] == "0xe80772eaf6e2e18b651f160bc9158b2a5cafca65")].index
    # bribe_df.loc[six_decimal_index, "bribe_amount"] = bribe_df.loc[six_decimal_index, "bribes"] * bribe_df.loc[six_decimal_index, "price"] / 1000000

    # nine_decimal_index = bribe_df[(bribe_df["address"] == "0x2952beb1326accbb5243725bd4da2fc937bca087")].index
    # bribe_df.loc[nine_decimal_index, "bribe_amount"] = bribe_df.loc[nine_decimal_index, "bribes"] * bribe_df.loc[nine_decimal_index, "price"] / 1000000000

    bribe_df = bribe_df.groupby(by="name")["bribe_amount"].sum().reset_index()
    bribe_df["epoch"] = epoch
    print(bribe_df)

    # Rewriting current Epoch's Bribe Data
    bribor = pd.read_csv(bribe_csv)
    drop_index = bribor[bribor["epoch"] == epoch].index
    index_list = drop_index.to_list()
    index_list = list(map(lambda x: x + 2, index_list))
    df_values = bribe_df.values.tolist()

    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["bribe_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    if index_list != []:
        worksheet1.delete_rows(index_list[0], index_list[-1])

    # Append to Worksheet
    gs.values_append("Master", {"valueInputOption": "USER_ENTERED"}, {"values": df_values})

    logger.info("Bribe Data Ended")
except Exception as e:
    logger.error("Error occurred during Bribe Data process. Error: %s" % e)
