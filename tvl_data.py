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

# V1
try:
    # Params Data
    fusion_api = config["api"]["fusion_api"]
    v1_subgraph = config["query"]["subgraph"]
    v1_mint_query = config["query"]["v1_mint_query"]
    v1_burn_query = config["query"]["v1_burn_query"]
    epoch_daily_csv = config["files"]["epoch_daily_data"]
    tvl_data_csv = config["files"]["tvl_data"]

    # Pulling TVL Data
    logger.info("TVL Data Started")

    # Get address data
    response = requests.get('https://api.thena.fi/api/v1/fusions')
    ids_df = pd.json_normalize(response.json()['data'])[['symbol', 'type', 'address']]
    ids_df.reset_index(inplace=True, drop=True)
    
    # Today and 2 Day Ago
    todayDate = datetime.utcnow()
    twodayago = todayDate - timedelta(10)
    my_time = datetime.min.time()
    my_datetime = datetime.combine(twodayago, my_time)
    timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())

    ids_v1_df = ids_df[(ids_df['type'] == 'Volatile') | (ids_df['type'] == 'Stable')]

    v1_df = pd.DataFrame()
    for symbol, address, pool_type in zip(ids_v1_df['symbol'], ids_v1_df['address'], ids_v1_df['type']):
        try:
            # Mints
            v1_mint_query["variables"]["pairAddress"] = address
            v1_mint_query["variables"]["startTime"] = timestamp
            for i in itertools.count(0, 100):
                v1_mint_query["variables"]["skip"] = i
                response = requests.post(v1_subgraph, json=v1_mint_query)
                data_mint = response.json()['data']['pairs']

                # Checking if empty data
                if not data_mint or data_mint[0]['mints'] == []:
                    break
                else:
                    df = pd.json_normalize(data_mint[0]['mints'])
                    df['Tx Type'] = 'Mint'
                    df['Pool Name'] = symbol
                    df['Pool Address'] = address
                    df['Pool Type'] = pool_type
                    v1_df = pd.concat([v1_df, df], axis=0, ignore_index=True)

            # Burns
            v1_burn_query["variables"]["pairAddress"] = address
            v1_burn_query["variables"]["startTime"] = timestamp
            for i in itertools.count(0, 100):
                v1_burn_query["variables"]["skip"] = i
                response = requests.post(v1_subgraph, json=v1_burn_query)
                data_burn = response.json()['data']['pairs']

                # Checking if empty data
                if not data_burn or data_burn[0]['burns'] == []:
                    break
                else:
                    df = pd.json_normalize(data_burn[0]['burns'])
                    df['Tx Type'] = 'Burn'
                    df['Pool Name'] = symbol
                    df['Pool Address'] = address
                    df['Pool Type'] = pool_type
                    v1_df = pd.concat([v1_df, df], axis=0, ignore_index=True)
        except Exception as e:
            logger.error("Error occurred during TVL Data process. Pair: %s, Address: %s, Error: %s" % (symbol, address, e))

    v1_df.reset_index(drop=True, inplace=True)

    logger.info("TVL Data Ended")
except Exception as e:
    logger.error("Error occurred during TVL Data process. Error: %s" % e, exc_info=True)
    
   
# Fusion
try:
    # Params Data
    id_data = config["files"]["id_data"]
    cl_subgraph = config["query"]["fusion_subgraph"]
    GRAPH_KEY = os.environ["GRAPH_KEY"]
    cl_mint_query = config["query"]["cl_mint_query"]
    cl_burn_query = config["query"]["cl_burn_query"]

    # Pulling TVL Data
    logger.info("TVL Data Fusion Started")

    # Request and Edit ID Data
    thena_ids_df = pd.read_csv(id_data)
    thena_ids_df['address'] = thena_ids_df['address'].str.lower()
    thena_ids_df['algebra_pool'] = thena_ids_df['algebra_pool'].str.lower()
    thena_ids_df = thena_ids_df[['address', 'algebra_pool']]
    ids_cl_df = ids_df[(ids_df['type'] != 'Volatile') & (ids_df['type'] != 'Stable')]
    ids_cl_df = ids_cl_df.merge(thena_ids_df, how='left')
    ids_cl_df.drop_duplicates(subset=['algebra_pool'], keep='first', inplace=True)
    
    # Today and 2 Day Ago
    todayDate = datetime.utcnow()
    twodayago = todayDate - timedelta(10)
    my_time = datetime.min.time()
    my_datetime = datetime.combine(twodayago, my_time)
    timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())

    if "[api-key]" in cl_subgraph:
        cl_subgraph = cl_subgraph.replace("[api-key]", GRAPH_KEY)

    addresses_to_skip = ["0x055557c6606f7b0d34e617653c447f079b0b0a73","0x90d43f6e920ab9500ae0473d6f67a95126ca4091","0x739e561786c920d025d57ed99be5d4eca3458e3e","0x7b879963ae083732f4514d564f4e4613e24e1f67","0x35f0c646a85675f31cfcd1e04d955cd2ce93e3c7","0x80c264189dd38f4fa5d6e424c1bf879b3b176076","0x130348553b3dea5d65767dc390eff257f9a9181d","0x088c568dc3123fc40dd153918125ee27027dd6e7","0x972e8be53425dbcaf3446c7ac130adb48ba3e125","0x1d56cbcc160d9f5fe56ba184bdb847dc209f7243","0xcc3aec37005fcc95288bfb046e5ae789cc322099","0x3cadd2f6a964d262b5dd5e7169c284b465336f0e","0xe6a2a77ca6b6c51103fbca83d3f171a920df42b4","0x1833de7f417952f54d465cf699f367bd94cd0d59","0xfb0e434eba0a467cd3f47cec5de63f4385861ea3","0xd20c7c2693c3bf844f84dfa03012a6c07032c5a6","0x604a99f4c5e46add74dba10c21b5e26374a1162f","0x16736fdab466f69e11ba5fc294be17d2fb8c3b02","0x0f28ae1eea69dda12bc89419f7e8552dd191c98e","0x733a0b28e4d7f2cb421730c4e4e26f2adce3d240","0x636f0d14e7f5f32a9a3773104d8608d561191a54","0x73a2b0fde4f8f8a2800fddcfd967a70b4b594abd","0xea66ad96abdb89cb28116f9e204e97a824cdff5b"]
    
    cl_df = pd.DataFrame()
    for symbol, address in zip(ids_cl_df['symbol'], ids_cl_df['algebra_pool']):
        try:
            # Skip
            if address.lower() in addresses_to_skip:
                continue
            # Mints
            cl_mint_query["variables"]["poolAddress"] = address
            cl_mint_query["variables"]["startTime"] = timestamp
            for i in itertools.count(0, 100):
                cl_mint_query["variables"]["skip"] = i
                response = requests.post(cl_subgraph, json=cl_mint_query)
                data_mint = response.json()['data']['pools']

                # Checking if empty data
                if not data_mint or data_mint[0]['mints'] == []:
                    break
                else:
                    df = pd.json_normalize(data_mint[0]['mints'])
                    df['Tx Type'] = 'Mint'
                    df['Pool Name'] = symbol
                    df['Pool Address'] = address
                    df['Pool Type'] = 'CL'
                    cl_df = pd.concat([cl_df, df], axis=0, ignore_index=True)

            # Burns
            cl_burn_query["variables"]["poolAddress"] = address
            cl_burn_query["variables"]["startTime"] = timestamp
            for i in itertools.count(0, 100):
                cl_burn_query["variables"]["skip"] = i
                response = requests.post(cl_subgraph, json=cl_burn_query)
                data_burn = response.json()['data']['pools']

                # Checking if empty data
                if not data_burn or data_burn[0]['burns'] == []:
                    break
                else:
                    df = pd.json_normalize(data_burn[0]['burns'])
                    df['Tx Type'] = 'Burn'
                    df['Pool Name'] = symbol
                    df['Pool Address'] = address
                    df['Pool Type'] = 'CL'
                    cl_df = pd.concat([cl_df, df], axis=0, ignore_index=True)
        except Exception as e:
            logger.error("Error occurred during TVL Data Fusion process. Pair: %s, Address: %s, Error: %s" % (symbol, address, e))

    cl_df.reset_index(drop=True, inplace=True)

    logger.info("TVL Data Fusion Ended")
except Exception as e:
    logger.error("Error occurred during TVL Data Fusion process. Error: %s" % e, exc_info=True)
    
    
    # Combined
try:
    logger.info("TVL Data Combined Started")

    # Data Manipulation
    tvl_df = pd.concat([v1_df, cl_df], ignore_index=True)

    if tvl_df.empty:
        raise Exception("Dataframe is empty")

    tvl_df['timestamp'] = pd.to_numeric(tvl_df['timestamp'])
    tvl_df['datetime'] = pd.to_datetime(tvl_df['timestamp'], unit='s')
    tvl_df['date'] = pd.to_datetime(tvl_df['timestamp'], unit='s').dt.date
    tvl_df['amountUSD'] = pd.to_numeric(tvl_df['amountUSD'])
    tvl_df["date"] = tvl_df["date"].apply(lambda date: datetime.strftime(date, "%Y-%m-%d"))
    tvl_df.drop(['datetime'], axis=1, inplace=True)
    tvl_df.sort_values("timestamp", ascending=True, inplace=True)

    tvl_df['TVL_inflow'] = tvl_df.apply(lambda row: row['amountUSD'] if row['Tx Type'] == 'Mint' else 0, axis=1)
    tvl_df['TVL_outflow'] = tvl_df.apply(lambda row: row['amountUSD'] if row['Tx Type'] == 'Burn' else 0, axis=1)
    tvl_df['TVL_change'] = tvl_df.apply(lambda row: row['amountUSD'] if row['Tx Type'] == 'Mint' else -row['amountUSD'], axis=1)   


    tvl_data_old = pd.read_csv(tvl_data_csv)
    drop_index = tvl_data_old[tvl_data_old['timestamp']>=timestamp].index
    index_list = drop_index.to_list()
    index_list = list(map(lambda x: x + 2, index_list))
    df_values = tvl_df.values.tolist()

    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["tvl_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    if index_list != []:
        worksheet1.delete_rows(index_list[0], index_list[-1])

    # Append to Worksheet
    gs.values_append("Master", {"valueInputOption": "USER_ENTERED"}, {"values": df_values})

    logger.info("TVL Data Combined Ended")
except Exception as e:
    logger.error("Error occurred during TVL Data Combined process. Error: %s" % e, exc_info=True)
