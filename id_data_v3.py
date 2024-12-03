import requests
import pandas as pd
from application_logging.logger import logger
from web3 import Web3
from web3.middleware import validation
import os, sys
from utils.helpers import read_params

# Params
params_path = 'params.yaml'
config = read_params(params_path)

try:
    logger.info("ID Data Started")

    # Params Data
    old_data = config["files"]["id_data"]
    provider_url = config["web3"]["provider_url"]
    provider_urls = config["web3"]["provider_urls"]
    id_abi = config["web3"]["id_abi"]
    fusion_api = config['api']['fusion_api']
    validation.METHODS_TO_VALIDATE = []

    # Old Data
    id_df_old = pd.read_csv(old_data)
    id_df_old['address'] = id_df_old['address'].str.lower()

    # New Data
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json',
    }

    data = requests.get(url=fusion_api, headers=headers, timeout=10, verify=True).json()['data']
    id_df_new = pd.json_normalize(data)[['symbol', 'address', 'type', 'gauge.address', 'gauge.fee', 'gauge.bribe']]
    id_df_new = id_df_new[['address']]
    id_df_new['address'] = id_df_new['address'].str.lower()

    # Merged Data & Processing
    id_df = pd.merge(id_df_old, id_df_new, how='right', on='address')
    id_df = id_df[id_df['type'].notna()]
    web3 = Web3(Web3.HTTPProvider(provider_urls[0], request_kwargs={'timeout': 2}))
    id_df['address'] = id_df['address'].apply(lambda x: web3.toChecksumAddress(x))
    id_df.to_csv("data/ids_data_v3.csv", index=False)

    logger.info("ID Data Ended")
except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    logger.error("Error occurred during ID Data process. Error: %s" % e, exc_info=True)