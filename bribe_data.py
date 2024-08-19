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
from utils.helpers import read_params

# Params
params_path = 'params.yaml'
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
    validation.METHODS_TO_VALIDATE = []

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
    bribes_list = []
    for name, bribe_ca in zip(ids_df["name"], ids_df["bribe_ca"]):
        if bribe_ca == "0x0000000000000000000000000000000000000000":
            continue
        for rpc_endpoint in provider_urls:
            try:
                w3 = Web3(Web3.HTTPProvider(rpc_endpoint, request_kwargs={"timeout": 5}))

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
                print(name)
                break
            except Exception as e:
                print(f"Error occurred while fetching bribes from {rpc_endpoint} for {name}: {e}")

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

    bribe_df = bribe_df.groupby(by="name")["bribe_amount"].sum().reset_index()
    bribe_df["epoch"] = epoch
    print(bribe_df)
    bribe_df.to_csv('bribe.csv', index=False)

    # Rewriting current Epoch's Bribe Data
    # bribor = pd.read_csv(bribe_csv)
    # drop_index = bribor[bribor["epoch"] == epoch].index
    # index_list = drop_index.to_list()
    # index_list = list(map(lambda x: x + 2, index_list))
    # df_values = bribe_df.values.tolist()

    # # Write to GSheets
    # credentials = os.environ["GKEY"]
    # credentials = json.loads(credentials)
    # gc = gspread.service_account_from_dict(credentials)

    # # Open a google sheet
    # sheetkey = config["gsheets"]["bribe_data_sheet_key"]
    # gs = gc.open_by_key(sheetkey)

    # # Select a work sheet from its name
    # worksheet1 = gs.worksheet("Master")
    # if index_list != []:
    #     worksheet1.delete_rows(index_list[0], index_list[-1])

    # # Append to Worksheet
    # gs.values_append("Master", {"valueInputOption": "USER_ENTERED"}, {"values": df_values})

    logger.info("Bribe Data Ended")
except Exception as e:
    logger.error("Error occurred during Bribe Data process. Error: %s" % e)
