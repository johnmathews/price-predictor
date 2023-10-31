# Databricks notebook source
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

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Retrieve API key
api_key = dbutils.secrets.get(scope="general", key="coinapi-api-key")

# Define time range for the last 3 years
end_date = datetime.now().strftime("%Y-%m-%d")
start_date = (datetime.now() - timedelta(days=3*365)).strftime("%Y-%m-%d")
params = {
  'period_id': '1DAY',
  'time_start': start_date,
  'time_end': end_date
}

headers = {
    #'Accept': 'text/plain', 
    'X-CoinAPI-Key': api_key
    }
response = requests.get(DATA_SOURCE, headers=headers, params=params)
response.status_code

# COMMAND ----------

btc_data = json.loads(response.text)
btc_data

# COMMAND ----------


# Convert to Spark DataFrame
btc_df = pd.DataFrame(btc_data)

btc_df

# COMMAND ----------

btc_spark_df = spark.createDataFrame(btc_df)

# Check if table exists and save
if spark._jsparkSession.catalog().tableExists(TABLE_NAME):
    btc_spark_df.write.mode("append").saveAsTable(TABLE_NAME)
else:
    btc_spark_df.write.saveAsTable(TABLE_NAME)
