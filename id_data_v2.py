import requests
import pandas as pd
import yaml
from application_logging.logger import logger
import itertools
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
    logger.info("ID Data Started")

    # Params Data
    subgraph = config["query"]["subgraph"]
    id_data_query = config["query"]["id_data_query"]
    provider_url = config["web3"]["provider_url"]
    amm_abi = config["web3"]["amm_abi"]
    ve_contract = config["web3"]["ve_contract"]
    voter_abi = config["web3"]["voter_abi"]
    cl_gauges_contract = config["web3"]["cl_gauges_contract"]
    cl_gauges_abi = config["web3"]["cl_gauges_abi"]
    cl_gauge_abi = config["web3"]["cl_gauge_abi"]
    cl_token_abi = config["web3"]["cl_token_abi"]

    # Request
    ids_df = pd.DataFrame()
    for i in itertools.count(0, 100):
        id_data_query["variables"]["skip"] = i
        response = requests.post(url=subgraph, json=id_data_query)
        data = response.json()["data"]["pairs"]

        if data == []:
            break
        else:
            temp_df = pd.json_normalize(data)
            ids_df = pd.concat([ids_df, temp_df], axis=0)
    ids_df.reset_index(drop=True, inplace=True)

    # Web3
    validation.METHODS_TO_VALIDATE = []
    w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 60}))

    names = []
    for address in ids_df["id"]:
        address = w3.toChecksumAddress(address)
        contract_instance = w3.eth.contract(address=address, abi=amm_abi)
        names.append({"name": contract_instance.functions.symbol().call(), "address": address})

    ids_df = pd.DataFrame(names)
    ids_df[["type", "pair"]] = ids_df["name"].str.split("-", 1, expand=True)
    ids_df.drop(["pair"], axis=1, inplace=True)

    xcad = ["0x8dDc543CB4Be74D8A4979DcCFC79C18BdEFd2Dad", "0x3Ec80A1f547ee6FD5D7FC0DC0C1525Ff343D087C", "0xe2d25A87e97a1016c35F736Ea77A4d6Bd1919E74", "0x46C69a989DC56747F007acfA4d6BC7c9FA8137bF", "0x6190E79064213E6A2997355153f57904FB4910C3", "0x3f4d5c667D918DcE84c89cC63998776d21081089", "0xd9F7096625B65Ac37c09d05c1c4548edfcCe9fBE", "0x3A569CeF2c6445d198F90d3D87DbFa3C7977cEb2", "0x2127bd04dDc9d8d1a4fAfCd96b4D4D81153831BC", "0xe200B5B3F08d1254d05d8797FFA1f99730C0025c", "0x6c03177eb78EbDe271B24FD6E446b301db1AEdb1", "0x7c7Ed76Dd95ffA7b899EE8Cc61aA8362d58aC420"]
    for pool in xcad:
        index = ids_df[ids_df["address"] == pool].index
        ids_df.loc[index, "name"] = ids_df.loc[index, "name"].values[0] + " OLD"

    # Solidly Pools
    contract_instance = w3.eth.contract(address=ve_contract, abi=voter_abi)
    gauges = []
    bribe_ca = []
    fee_ca = []
    for address in ids_df["address"]:
        address = w3.toChecksumAddress(address)
        gauge = contract_instance.functions.gauges(address).call()
        gauges.append(gauge)
        bribe_ca.append(contract_instance.functions.external_bribes(gauge).call())
        fee_ca.append(contract_instance.functions.internal_bribes(gauge).call())
    ids_df["gauges"] = gauges
    ids_df["bribe_ca"] = bribe_ca
    ids_df["fee_ca"] = fee_ca

    # CL Pools
    contract_instance = w3.eth.contract(address=cl_gauges_contract, abi=cl_gauges_abi)
    cl_gauges = contract_instance.functions.gauges().call()
    tokens = []
    bribe_ca = []
    fee_ca = []
    for gauge in cl_gauges:
        contract_instance = w3.eth.contract(address=gauge, abi=cl_gauge_abi)
        tokens.append(contract_instance.functions.TOKEN().call())
        bribe_ca.append(contract_instance.functions.external_bribe().call())
        fee_ca.append(contract_instance.functions.internal_bribe().call())
    name = []
    for token in tokens:
        contract_instance = w3.eth.contract(address=token, abi=cl_token_abi)
        name.append(contract_instance.functions.symbol().call() + " " + str(token[-4]))
        

    cl_df = pd.DataFrame(
        {'name' : name,
        'address' : tokens,
        'gauges' : cl_gauges,
        'bribe_ca' : bribe_ca,
        'fee_ca' : fee_ca}
    )
    cl_df['type'] = "CL"
    cl_df = cl_df[['name', 'address', 'type', 'gauges', 'bribe_ca', 'fee_ca']]
    ids_df = pd.concat([ids_df, cl_df], axis=0)
    ids_df.reset_index(drop=True, inplace=True)

    ids_df.to_csv("data/ids_data_v2.csv", index=False)

    logger.info("ID Data Ended")
except Exception as e:
    logger.error("Error occurred during ID Data process. Error: %s" % e)
