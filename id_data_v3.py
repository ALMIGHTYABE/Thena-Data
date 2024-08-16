import requests
import pandas as pd
import yaml
from application_logging.logger import logger
import itertools
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
    id_df_old = id_df_old[['name', 'address', 'type', 'algebra_pool']]

    # New Data
    data = requests.get(url=fusion_api).json()['data']
    id_df_new = pd.json_normalize(data)[['symbol', 'address', 'type', 'gauge.address', 'gauge.fee', 'gauge.bribe']]
    id_df_new['new_name'] = id_df_new['symbol'] + " - " + id_df_new['type']
    id_df_new.columns = ['new_symbol', 'address', 'new_type', 'gauges', 'fee_ca', 'bribe_ca', 'new_name']
    id_df_new['address'] = id_df_new['address'].str.lower()

    # Merged Data & Processing
    id_df = pd.merge(id_df_old, id_df_new, how='right', on='address')

    cl_df = id_df[id_df.new_type.isin(['Narrow', 'Wide', 'ICHI', 'DefiEdge', 'Correlated', 'CL_Stable'])].copy()
    cl_df = cl_df[cl_df['algebra_pool'].isna()]

    algebra_pool = []
    for idx, row in cl_df.iterrows():
        for rpc_endpoint in provider_urls:
            try:
                web3 = Web3(Web3.HTTPProvider(rpc_endpoint, request_kwargs={'timeout': 2}))
                contract = web3.eth.contract(address=Web3.toChecksumAddress(row['address']), abi=id_abi)
                pool_address = contract.functions.pool().call()
                algebra_pool.append(pool_address)
                break
            except Exception as e:
                logger.error("Error occurred during ID Data process for address %s. Error: %s" % (row['address'], e), exc_info=True)
        algebra_pool.append(None)

    cl_df.loc[:, 'algebra_pool'] = algebra_pool
    id_df.loc[cl_df.index, 'algebra_pool'] = cl_df['algebra_pool']
    duplicate_new_names = id_df[id_df.duplicated(subset='new_name', keep=False)]
    web3 = Web3(Web3.HTTPProvider(provider_urls[0], request_kwargs={'timeout': 2}))
    id_df['address'] = id_df['address'].apply(lambda x: web3.toChecksumAddress(x))
    id_df.loc[duplicate_new_names.index, 'new_name'] = (duplicate_new_names['new_name'] + " " + duplicate_new_names['address'].str[-4:])

    id_df.to_csv("data/ids_data_v3.csv", index=False)

    logger.info("ID Data Ended")
except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    logger.error("Error occurred during ID Data process. Error: %s" % e, exc_info=True)