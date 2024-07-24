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

# # V1
# try:
#     # Params Data
#     subgraph = config["query"]["subgraph"]
#     id_data = config["files"]["id_data"]
#     pair_data_query = config["query"]["pair_data_query"]
#     epoch_daily_csv = config["files"]["epoch_daily_data"]
#     pair_data_csv = config["files"]["pair_data"]
#     provider_url = config["web3"]["provider_url"]

#     # Pulling Pair Data
#     logger.info("Pair Data Started")

#     # Request and Edit Pair Data
#     ids_df = pd.read_csv(id_data)
#     ids_df = ids_df[ids_df['type'] != "CL"]
    
#     # Today and 2 Day Ago
#     todayDate = datetime.utcnow()
#     twodayago = todayDate - timedelta(12)
#     my_time = datetime.min.time()
#     my_datetime = datetime.combine(twodayago, my_time)
#     timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())

#     # Web3
#     validation.METHODS_TO_VALIDATE = []
#     w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 5}))

#     pairdata_df = pd.DataFrame()
#     for name, contract_address in zip(ids_df["name"], ids_df["address"]):
#         try:
#             pair_data_query["variables"]["pairAddress"] = contract_address
#             pair_data_query["variables"]["startTime"] = timestamp
#             response = requests.post(subgraph, json=pair_data_query, timeout=5)
#             data = response.json()["data"]["pairDayDatas"]
#             df = pd.json_normalize(data)
#             df["name"] = name
#             pairdata_df = pd.concat([pairdata_df, df], axis=0, ignore_index=True)
#             pairdata_df.reset_index(drop=True, inplace=True)
#         except Exception as e:
#             logger.error("Error occurred during Pair Data process. Pair: %s, Address: %s, Error: %s" % (name, contract_address, e))

#     epoch_data = pd.read_csv(epoch_daily_csv)
#     epoch_data["date"] = epoch_data["date"].apply(lambda date: datetime.strptime(date, "%d-%m-%Y").date())

#     pairdata_df["date"] = pairdata_df["date"].apply(lambda timestamp: datetime.utcfromtimestamp(timestamp).date())
#     pairdata_df = pd.merge(pairdata_df, ids_df[["name", "address", "type"]], how="left", on="name")
#     pairdata_df = pd.merge(pairdata_df, epoch_data[["date", "epoch"]], how="left", on="date")

#     pairdata_df["fee %"] = pairdata_df["type"]
#     pairdata_df["fee %"].replace({"vAMM": 0.20, "sAMM": 0.01}, inplace=True)

#     pairdata_df["dailyVolumeUSD"] = pd.to_numeric(pairdata_df["dailyVolumeUSD"])
#     pairdata_df["fee"] = (pairdata_df["dailyVolumeUSD"] * pairdata_df["fee %"]) / 100
#     pairdata_df.sort_values("date", ascending=True, inplace=True)
#     pairdata_df["date"] = pairdata_df["date"].apply(lambda date: datetime.strftime(date, "%Y-%m-%d"))
#     pairdata_df = pairdata_df[['id', 'date', 'dailyVolumeToken0', 'dailyVolumeToken1', 'dailyVolumeUSD', 'reserveUSD', '__typename', 'name', 'address', 'type', 'epoch', 'fee %', 'fee']]
    
#     pairdata_old = pd.read_csv(pair_data_csv)
#     drop_index = pairdata_old[pairdata_old['date']>datetime.fromtimestamp(timestamp).strftime(format='%Y-%m-%d')].index
#     index_list = drop_index.to_list()
#     index_list = list(map(lambda x: x + 2, index_list))
#     pairdata_df['__typename'] = 'V1'
#     df_values = pairdata_df.values.tolist()

#     if pairdata_df.empty:
#         raise Exception("Dataframe is empty")

#     # Write to GSheets
#     credentials = os.environ["GKEY"]
#     credentials = json.loads(credentials)
#     gc = gspread.service_account_from_dict(credentials)

#     # Open a google sheet
#     sheetkey = config["gsheets"]["pair_data_sheet_key"]
#     gs = gc.open_by_key(sheetkey)

#     # Select a work sheet from its name
#     worksheet1 = gs.worksheet("Master")
#     if index_list != []:
#         worksheet1.delete_rows(index_list[0], index_list[-1])

#     # Append to Worksheet
#     gs.values_append("Master", {"valueInputOption": "USER_ENTERED"}, {"values": df_values})

#     logger.info("Pair Data Ended")
# except Exception as e:
#     logger.error("Error occurred during Pair Data process. Error: %s" % e, exc_info=True)
    
   
# Fusion
try:
    # Params Data
    subgraph = config["query"]["fusion_subgraph"]
    GRAPH_KEY = os.environ["GRAPH_KEY"]
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
    twodayago = todayDate - timedelta(12)
    my_time = datetime.min.time()
    my_datetime = datetime.combine(twodayago, my_time)
    timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())

    # Web3
    validation.METHODS_TO_VALIDATE = []
    w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 5}))
    
    addresses_to_skip = ["0x055557c6606f7b0d34e617653c447f079b0b0a73","0x90d43f6e920ab9500ae0473d6f67a95126ca4091","0x739e561786c920d025d57ed99be5d4eca3458e3e","0x7b879963ae083732f4514d564f4e4613e24e1f67","0x35f0c646a85675f31cfcd1e04d955cd2ce93e3c7","0x80c264189dd38f4fa5d6e424c1bf879b3b176076","0x130348553b3dea5d65767dc390eff257f9a9181d","0x088c568dc3123fc40dd153918125ee27027dd6e7","0x972e8be53425dbcaf3446c7ac130adb48ba3e125","0x1d56cbcc160d9f5fe56ba184bdb847dc209f7243","0xcc3aec37005fcc95288bfb046e5ae789cc322099","0x3cadd2f6a964d262b5dd5e7169c284b465336f0e","0xe6a2a77ca6b6c51103fbca83d3f171a920df42b4","0x1833de7f417952f54d465cf699f367bd94cd0d59","0xfb0e434eba0a467cd3f47cec5de63f4385861ea3","0xd20c7c2693c3bf844f84dfa03012a6c07032c5a6","0x604a99f4c5e46add74dba10c21b5e26374a1162f","0x16736fdab466f69e11ba5fc294be17d2fb8c3b02","0x0f28ae1eea69dda12bc89419f7e8552dd191c98e","0x733a0b28e4d7f2cb421730c4e4e26f2adce3d240","0x636f0d14e7f5f32a9a3773104d8608d561191a54","0x73a2b0fde4f8f8a2800fddcfd967a70b4b594abd","0xea66ad96abdb89cb28116f9e204e97a824cdff5b"]

    pairdata_fusion_df = pd.DataFrame()
    for name, contract_address in zip(ids_df["name"], ids_df["algebra_pool"]):
        try:
            if contract_address.lower() in addresses_to_skip:
                continue
            pair_data_fusion_query["variables"]["pairAddress"] = contract_address.lower()
            pair_data_fusion_query["variables"]["startTime"] = timestamp
            subgraph = subgraph.replace("[api-key]", GRAPH_KEY)
            response = requests.post(subgraph, json=pair_data_fusion_query, timeout=5)
            data = response.json()["data"]["poolDayDatas"]
            df = pd.json_normalize(data)
            df = df[['id', 'date', 'tvlUSD', 'volumeUSD', 'volumeToken0', 'volumeToken1', 'token0Price', 'token1Price', 'feesUSD', '__typename']]
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
    pairdata_fusion_df = pairdata_fusion_df[['id', 'date', 'tvlUSD', 'volumeUSD', 'volumeToken0', 'volumeToken1', 'token0Price', 'token1Price', 'feesUSD', '__typename', 'name', 'algebra_pool', 'type', 'epoch']]
    
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

