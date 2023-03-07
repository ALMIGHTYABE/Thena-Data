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
    subgraph = config["data"]["subgraph"]
    myobj1 = config["data"]["id_data_query"]
    provider_url = config["data"]["provider_url"]
    abi1 = config["data"]["abi1"]
    abi2 = config["data"]["abi2"]
    abi3 = config["data"]["abi3"]
    ve_contract = config["data"]["ve_contract"]
    epoch_csv = config["data"]["epoch_data"]
    price_api = config["data"]["price_api"]
    bribe_csv = config["data"]["bribe_data"]

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
    w3 = Web3(Web3.HTTPProvider(provider_url))

    names = []
    for address in ids_df["id"]:
        address = w3.toChecksumAddress(address)
        contract_instance = w3.eth.contract(address=address, abi=abi1)
        names.append({"name": contract_instance.functions.symbol().call(), "address": address})

    ids_df = pd.DataFrame(names)
    ids_df[["type", "pair"]] = ids_df["name"].str.split("-", 1, expand=True)
    ids_df.drop(["pair"], axis=1, inplace=True)

    logger.info("ID Data Ended")

    # Pulling Bribe Data
    logger.info("Bribe Data Started")

    # Web3
    contract_instance = w3.eth.contract(address=ve_contract, abi=abi2)

    gauges = []
    bribe_ca = []
    for address in ids_df["address"]:
        address = w3.toChecksumAddress(address)
        gauge = contract_instance.functions.gauges(address).call()
        gauges.append(gauge)
        bribe_ca.append(contract_instance.functions.external_bribes(gauge).call())
    ids_df["gauges"] = gauges
    ids_df["bribe_ca"] = bribe_ca

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
            pass
        else:
            contract_address = bribe_ca
            contract_instance = w3.eth.contract(address=contract_address, abi=abi3)

            rewardsListLength = contract_instance.functions.rewardsListLength().call()

            rewardTokens = []
            for reward_num in range(rewardsListLength):
                rewardTokens.append(contract_instance.functions.rewardTokens(reward_num).call())

            for reward_addy in rewardTokens:
                rewarddata = contract_instance.functions.rewardData(reward_addy, timestamp).call()
                if rewarddata[1] > 0:
                    bribes_list.append({"name": name, "bribes": rewarddata[1], "address": reward_addy})

    bribe_df = pd.DataFrame(bribes_list)
    bribe_df["address"] = bribe_df["address"].apply(str.lower)

    # Pull Prices
    response = requests.get(price_api)
    pricelist = []
    for i in response.json()["data"]:
        pricelist.append([i["name"], i["address"], i["price"]])

    price_df = pd.DataFrame(pricelist, columns=["name", "address", "price"])

    # Bribe Amounts
    bribe_df = bribe_df.merge(price_df[["address", "price"]], on="address", how="left")
    bribe_df["bribe_amount"] = np.where((bribe_df["address"] != "0xe80772eaf6e2e18b651f160bc9158b2a5cafca65") | (bribe_df["address"] != "0x71be881e9c5d4465b3fff61e89c6f3651e69b5bb"), bribe_df['bribe_amount'] / 1000000000000000000, bribe_df['bribe_amount'])
    
    four_decimal_index = bribe_df[(bribe_df["address"] == "0x71be881e9c5d4465b3fff61e89c6f3651e69b5bb")].index
    bribe_df.loc[four_decimal_index, "bribe_amount"] = bribe_df.loc[four_decimal_index, "bribes"] * bribe_df.loc[four_decimal_index, "price"] / 10000
    
    six_decimal_index = bribe_df[(bribe_df["address"] == "0xe80772eaf6e2e18b651f160bc9158b2a5cafca65")].index
    bribe_df.loc[six_decimal_index, "bribe_amount"] = bribe_df.loc[six_decimal_index, "bribes"] * bribe_df.loc[six_decimal_index, "price"] / 1000000

    print(bribe_df)
    bribe_df = bribe_df.groupby(by="name")["bribe_amount"].sum().reset_index()
    bribe_df["epoch"] = epoch
    print(bribe_df)

    # Rewriting current Epoch's Bribe Data
    bribor = pd.read_csv(bribe_csv)
    current_bribe_index = bribor[bribor["epoch"] == epoch].index
    bribor.drop(current_bribe_index, inplace=True)
    bribe_df = pd.concat([bribor, bribe_df], ignore_index=True, axis=0)

    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["data"]["sheetkey3"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    worksheet1.clear()
    set_with_dataframe(
        worksheet=worksheet1,
        dataframe=bribe_df,
        include_index=False,
        include_column_header=True,
        resize=True,
    )

    logger.info("Bribe Data Ended")
except Exception as e:
    logger.error("Error occurred during Bribe Data process. Error: %s" % e)
