import requests
import pandas as pd
import yaml
import json
import os
from datetime import datetime, timezone
from application_logging.logger import logger
import jmespath
import gspread
from web3 import Web3
from web3.middleware import validation
from utils.helpers import read_params

# Params
params_path = 'params.yaml'
config = read_params(params_path)

try:
    logger.info("Emissions Data Started")

    # Params Data
    id_data = config["files"]["id_data"]
    epoch_csv = config["files"]["epoch_data"]
    price_api = config["api"]["price_api"]
    provider_url = config["web3"]["provider_url"]
    gauge_abi = config["web3"]["gauge_abi"]
    bribe_abi = config["web3"]["bribe_abi"]

    # Get Epoch Timestamp
    todayDate = datetime.utcnow()
    my_time = datetime.min.time()
    my_datetime = datetime.combine(todayDate, my_time)
    timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
    print("Today's date:", my_datetime, timestamp)

    # Read Epoch Data
    epoch_data = pd.read_csv(epoch_csv)
    epoch = epoch_data[epoch_data["timestamp"] == timestamp]["epoch"].values[0]

    # Read IDS Data
    ids_df = pd.read_csv(id_data)
    ids_df["epoch"] = epoch

    # Web3
    validation.METHODS_TO_VALIDATE = []
    w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 60}))

    weeklyreward = []
    for gauge in ids_df["gauges"]:
        if gauge == "0x0000000000000000000000000000000000000000":
            weeklyreward.append(0)
        else:
            contract_instance = w3.eth.contract(address=gauge, abi=gauge_abi)
            weeklyreward.append(contract_instance.functions.rewardForDuration().call() / 1000000000000000000)

    ids_df["emissions"] = weeklyreward
    
    voteweight = []
    for bribe in ids_df["bribe_ca"]:
        if bribe == "0x0000000000000000000000000000000000000000":
            voteweight.append(0)
        else:
            contract_instance = w3.eth.contract(address=bribe, abi=bribe_abi)
            voteweight.append(contract_instance.functions._totalSupply(timestamp).call() / 1000000000000000000)
                          
    ids_df["voteweight"] = voteweight
    
    # Pull Prices
    response = requests.get(price_api)
    THE_price = jmespath.search("data[?name == 'THENA'].price", response.json())[0]

    # Cleanup
    ids_df["THE_price"] = THE_price
    ids_df["value"] = ids_df["emissions"] * ids_df["THE_price"]
    ids_df = ids_df[["epoch", "name", "voteweight","emissions", "value", "THE_price"]]
    ids_df = ids_df[ids_df["voteweight"] > 0]
    df_values = ids_df.values.tolist()

    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["emissions_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Append to Worksheet
    gs.values_append("Master", {"valueInputOption": "RAW"}, {"values": df_values})

    logger.info("Emissions Data Ended")
except Exception as e:
    logger.error("Error occurred during Emissions Data process. Error: %s" % e)
