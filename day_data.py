import requests
import pandas as pd
import numpy as np
import yaml
import os
from datetime import datetime
from application_logging.logger import logger
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# Params
params_path = "params.yaml"


def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


config = read_params(params_path)

try:
    logger.info("Process Started")

    # Params Data
    subgraph = config["data"]["subgraph"]
    myobj = config["data"]["day_data_query"]

    # Request
    response = requests.post(url=subgraph, json=myobj)
    data = response.json()["data"]["dayDatas"]
    df = pd.json_normalize(data)
    df["date"] = df["date"].apply(
        lambda timestamp: datetime.utcfromtimestamp(timestamp).date()
    )
    df["epoch"] = np.divmod(np.arange(len(df)), 7)[0]

    # Write to GSheets
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    key = os.environ["GKEY"]
    credentials = Credentials.from_service_account_file(filename=key, scopes=scopes)
    gc = gspread.authorize(credentials)

    gauth = GoogleAuth()
    drive = GoogleDrive(gauth)

    # Open a google sheet
    sheetkey = config["data"]["sheetkey"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    worksheet1.clear()
    set_with_dataframe(
        worksheet=worksheet1,
        dataframe=df,
        include_index=False,
        include_column_header=True,
        resize=True,
    )

    logger.info("Process Ended")
except Exception as e:
    logger.error("Error occurred during the process. Error: %s" % e)
