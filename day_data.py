import requests
import pandas as pd
import json
import os
import time
from datetime import datetime, timezone, date, timedelta
from application_logging.logger import logger
import gspread
from gspread_dataframe import set_with_dataframe
from utils.helpers import read_params

# Params
params_path = 'params.yaml'
config = read_params(params_path)
daydelta = config['delta']['day_data']

# V1
try:
    logger.info("Day Data Started")

    # Params Data
    subgraph = config["query"]["subgraph"]
    day_data_query = config["query"]["day_data_query"]
    daily_data_csv = config["files"]["daily_data"]
    
    # Date Stuff
    todayDate = datetime.utcnow()
    twodayago = todayDate - timedelta(daydelta)
    my_time = datetime.min.time()
    my_datetime = datetime.combine(twodayago, my_time)
    timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
    
    # Request
    day_data_query["variables"]["startTime"] = timestamp
    response = requests.post(url=subgraph, json=day_data_query)
    data = response.json()["data"]["dayDatas"]
    day_data_df = pd.DataFrame(data)
    day_data_df = day_data_df[['id', 'date', 'totalVolumeUSD', 'dailyVolumeUSD', 'dailyVolumeETH', 'totalLiquidityUSD', 'totalLiquidityETH', '__typename']]
    day_data_df["date"] = day_data_df["date"].apply(lambda timestamp: datetime.utcfromtimestamp(timestamp).date())
    day_data_df["date"] = day_data_df["date"].apply(lambda date: datetime.strftime(date, "%Y-%m-%d"))
    
    day_data_old = pd.read_csv(daily_data_csv)
    drop_index = day_data_old[day_data_old['date']>datetime.fromtimestamp(timestamp).strftime(format='%Y-%m-%d')].index
    index_list = drop_index.to_list()
    index_list = list(map(lambda x: x + 2, index_list))
    day_data_df['__typename'] = 'V1'
    df_values = day_data_df.values.tolist()
    
    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["daily_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    if index_list != []:
        worksheet1.delete_rows(index_list[0], index_list[-1])

    retries, delay = 3, 30
    for attempt in range(retries):
        try:
            gs = gc.open_by_key(sheetkey)
            # Append to Worksheet
            gs.values_append("Master", {"valueInputOption": "USER_ENTERED"}, {"values": df_values})
            logger.error("Data successfully appended to Google Sheets.")
            break  # Break the loop if successful
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            if attempt < retries - 1:
                logger.error(f"Retrying in {delay} seconds... (Attempt {attempt + 2}/{retries})")
                time.sleep(delay)  # Wait before retrying
            else:
                logger.error("All retries failed.")
                raise  # Re-raise the exception if retries are exhausted

    logger.info("Day Data Ended")
except Exception as e:
    logger.error("Error occurred during Day Data process. Error: %s" % e)
    
# Fusion   
try:
    logger.info("Day Data Fusion Started")

    # Params Data
    subgraph = config["query"]["fusion_subgraph"]
    GRAPH_KEY = os.environ["GRAPH_KEY"]
    day_data_fusion_query = config["query"]["day_data_fusion_query"]
    daily_data_fusion_csv = config["files"]["daily_data_fusion"]
    
    # Date Stuff
    todayDate = datetime.utcnow()
    twodayago = todayDate - timedelta(10)
    my_time = datetime.min.time()
    my_datetime = datetime.combine(twodayago, my_time)
    timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
    
    # Request
    day_data_fusion_query["variables"]["startTime"] = timestamp
    if "[api-key]" in subgraph:
        subgraph = subgraph.replace("[api-key]", GRAPH_KEY)
    response = requests.post(url=subgraph, json=day_data_fusion_query)
    try:
        data = response.json()["data"]["fusionDayData"]
    except:
        logger.info(response.json())
    day_data_fusion_df = pd.DataFrame(data)
    day_data_fusion_df['feesUSD'] = 0
    day_data_fusion_df = day_data_fusion_df[['id', 'date', 'volumeUSD', 'feesUSD', 'tvlUSD', '__typename']]
    day_data_fusion_df["date"] = day_data_fusion_df["date"].apply(lambda timestamp: datetime.utcfromtimestamp(timestamp).date())
    day_data_fusion_df["date"] = day_data_fusion_df["date"].apply(lambda date: datetime.strftime(date, "%Y-%m-%d"))
    
    day_data_fusion_old = pd.read_csv(daily_data_fusion_csv)
    drop_index = day_data_fusion_old[day_data_fusion_old['date']>datetime.fromtimestamp(timestamp).strftime(format='%Y-%m-%d')].index
    index_list = drop_index.to_list()
    index_list = list(map(lambda x: x + 2, index_list))
    day_data_fusion_df['__typename'] = 'Fusion'
    df_values = day_data_fusion_df.values.tolist()
    
    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["daily_data_fusion_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    if index_list != []:
        worksheet1.delete_rows(index_list[0], index_list[-1])

    retries, delay = 3, 30
    for attempt in range(retries):
        try:
            gs = gc.open_by_key(sheetkey)
            # Append to Worksheet
            gs.values_append("Master", {"valueInputOption": "USER_ENTERED"}, {"values": df_values})
            logger.error("Data successfully appended to Google Sheets.")
            break  # Break the loop if successful
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            if attempt < retries - 1:
                logger.error(f"Retrying in {delay} seconds... (Attempt {attempt + 2}/{retries})")
                time.sleep(delay)  # Wait before retrying
            else:
                logger.error("All retries failed.")
                raise  # Re-raise the exception if retries are exhausted

    logger.info("Day Data Fusion Ended")
except Exception as e:
    logger.error("Error occurred during Day Data Fusion process. Error: %s" % e)
    
    
    # Combined  
try:
    logger.info("Day Data Combined Started")

    # Data Manipulation
    day_data_old = pd.read_csv(daily_data_csv)
    day_data_fusion_old = pd.read_csv(daily_data_fusion_csv)
    df1 = day_data_old[['id', 'date', 'dailyVolumeUSD', 'totalLiquidityUSD', '__typename']]
    df2 = day_data_fusion_old[['id', 'date', 'volumeUSD', 'tvlUSD', '__typename']]
    df2.columns = ['id', 'date', 'dailyVolumeUSD', 'totalLiquidityUSD', '__typename']
    day_data_combined_df = pd.concat([df1, df2], ignore_index=True, axis=0)
    
    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["daily_data_combined_sheet_key"]

    retries, delay = 3, 30
    for attempt in range(retries):
        try:
            gs = gc.open_by_key(sheetkey)
            # Select a work sheet from its name
            worksheet1 = gs.worksheet("Master")
            worksheet1.clear()
            # Add to Worksheet
            set_with_dataframe(
                worksheet=worksheet1,
                dataframe=day_data_combined_df,
                include_index=False,
                include_column_header=True,
                resize=True,
            )
            logger.error("Data successfully added to Google Sheets.")
            break  # Break the loop if successful
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            if attempt < retries - 1:
                logger.error(f"Retrying in {delay} seconds... (Attempt {attempt + 2}/{retries})")
                time.sleep(delay)  # Wait before retrying
            else:
                logger.error("All retries failed.")
                raise  # Re-raise the exception if retries are exhausted

    logger.info("Day Data Combined Ended")
except Exception as e:
    logger.error("Error occurred during Day Data Combined process. Error: %s" % e)
