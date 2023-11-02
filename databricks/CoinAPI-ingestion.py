# Databricks notebook source
# MAGIC %config InteractiveShell.ast_node_interactivity='all'

# COMMAND ----------

import configparser

def read_ini_config_from_workspace(path):
    """
    Read an .ini configuration file from the Databricks workspace and return a ConfigParser object.
    """
    
    # Parse the content using ConfigParser
    config = configparser.ConfigParser()
    
    # Use dbutils to read the file content
    config_content = dbutils.fs.head(path)
    config.read_string(config_content)
    
    return config


config_path = "/Workspace/price-predictor/config/config.ini"
config = read_ini_config_from_workspace(config_path)
START_DATE = config.get("General", "start_date")
START_DATE

# COMMAND ----------

from pyspark.sql import SparkSession
import requests
import json
from datetime import datetime, timedelta
import pandas as pd

EXCHANGE = "COINBASE"
ASSET = "BTC"
BASE_CURRENCY = "USD"
TABLE_NAME = "btc_usd_daily_price"
DATA_SOURCE = f"https://rest.coinapi.io/v1/ohlcv/{EXCHANGE}_SPOT_{ASSET}_{BASE_CURRENCY}/history"

# Define time range for the last 3 years
START_DATE = (datetime.now() - timedelta(days=3*365)).strftime("%Y-%m-%d")
END_DATE = datetime.now().strftime("%Y-%m-%d")

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Retrieve API key
api_key = dbutils.secrets.get(scope="general", key="coinapi-api-key")


params = {
  'period_id': '1DAY',
  'time_start': START_DATE,
  'time_end': END_DATE,
}

headers = {
    #'Accept': 'text/plain', 
    'X-CoinAPI-Key': api_key
    }
response = requests.get(DATA_SOURCE, headers=headers, params=params)
response.status_code

# COMMAND ----------

btc_data = json.loads(response.text)

# COMMAND ----------


# Convert to Spark DataFrame
btc_df = pd.DataFrame(btc_data)

btc_df.size
btc_df.head()

# COMMAND ----------

btc_spark_df = spark.createDataFrame(btc_df)

# Check if table exists and save
if spark._jsparkSession.catalog().tableExists(TABLE_NAME):
    btc_spark_df.write.mode("append").saveAsTable(TABLE_NAME)
else:
    btc_spark_df.write.saveAsTable(TABLE_NAME)
