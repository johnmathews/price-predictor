# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook magic

# COMMAND ----------

# MAGIC %config InteractiveShell.ast_node_interactivity='all'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project configuration values

# COMMAND ----------

import configparser

CONFIG_PATH = '../config/config.ini'

def read_ini_config_from_workspace(path):
    """
    Read an .ini configuration file from the Databricks workspace and return a dictionary of configurations.
    """
    config_object = configparser.ConfigParser()
    with open(path, "r") as file:
        config_object.read_file(file)
    
    # Create a nested dictionary for each section and its key-value pairs
    config_dict = {section: dict(config_object.items(section)) for section in config_object.sections()}
    
    return config_dict

def get_config_value(config, section, key):
    """
    Retrieve a specific configuration value given the section and key.
    """
    return config.get(section, {}).get(key)

config = read_ini_config_from_workspace(CONFIG_PATH)
START_DATE = get_config_value(config, "General", "start_date")
DATABASE_NAME = get_config_value(config, "General", "database_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook constants

# COMMAND ----------

## imports and local config
from pyspark.sql import SparkSession

import nasdaqdatalink

from pyspark.sql.functions import col, count, datediff, row_number, min, max, date_format

import os
import json
import requests
from datetime import datetime, timedelta
import pandas as pd

ALPHA_VANTAGE_API_KEY: str = dbutils.secrets.get(scope="general", key="alphavantage-api-key")
ENDPOINT = 'https://www.alphavantage.co/query'
DATE_COLUMN: str = "DATE"
DAYS_WITH_MISSING_DATA: int = 0 
today: datetime = datetime.now().strftime("%Y-%m-%d")

# COMMAND ----------

# MAGIC %md
# MAGIC # Notes
# MAGIC - FRED = Federal Reserve Economic Data
# MAGIC - GDP = Gross Domestic Product
# MAGIC

# COMMAND ----------


def get_historical_ohlcv(ticker_symbol: str, output_size: str):
    
    params = {
        'function': 'TIME_SERIES_DAILY',  # Use 'TIME_SERIES_DAILY_ADJUSTED' for adjusted close values
        'symbol': ticker_symbol,
        'outputsize': output_size,  # Use 'compact' for the latest 100 data points; 'full' returns up to 20 years of historical data
        'apikey': ALPHA_VANTAGE_API_KEY
    }
    
    response = requests.get(ENDPOINT, params=params)
    data = response.json()
    
    # Check if the response contains an error message
    if "Error Message" in data:
        raise ValueError(f"Error retrieving data for {ticker_symbol}: {data['Error Message']}")
    
    # The historical data is under the 'Time Series (Daily)' key
    ohlcv_data = data.get('Time Series (Daily)', {})
    
    # Return the data as a dictionary
    return ohlcv_data


# COMMAND ----------

alpha_vantage_tickers = {
    "SPY": {
        "ticker_symbol": "SPY",
        "notes": "Alpha Vantage - SPDR S&P 500 ETF Trust",
    },
    "QQQ": {
        "ticker_symbol": "QQQ",
        "notes": "Alpha Vantage - Invesco QQQ Trust, which tracks the NASDAQ-100, a subset of the NASDAQ Composite",
    },
    "DIA": {
        "ticker_symbol": "DIA",
        "notes": "Alpha Vantage - SPDR Dow Jones Industrial Average ETF",
    },
    "FTSE100": {
        "ticker_symbol": "ISF.L",
        "notes": "Alpha Vantage - iShares Core FTSE 100 UCITS ETF (traded on the London Stock Exchange)",
    },
    "Nikkei225": {
        "ticker_symbol": "EWJ",
        "notes": "Alpha Vantage - iShares MSCI Japan ETF (tracks a broad range of Japanese stocks)",
    },
    "DAX": {
        "ticker_symbol": "DAX",
        "notes": "Alpha Vantage -  Global X DAX Germany ETF",
    },
    "CAC40": {
        "ticker_symbol": "CAC",
        "notes": "Alpha Vantage - Lyxor CAC 40 ETF (traded on Euronext Paris)",
    },
}

# COMMAND ----------

for key in alpha_vantage_tickers.keys():
    info = alpha_vantage_tickers[key]
    ticker = info["ticker_symbol"]
    notes = info["notes"]
    

spy_data = get_historical_ohlcv("SPY")

# COMMAND ----------

df = pd.DataFrame.from_dict(spy_data, orient="index")
df = df.reset_index().rename(columns={'index': 'date'})
df.head()
df.tail()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark setup

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# If TABLE_NAME doesnt exist, go get data. If it does exist, figure out what data to request

def table_exists(spark: SparkSession, table_name: str, database: str) -> bool:
    """
    Check if a table exists in the given database.
    """
    table_name = table_name.lower()
    tables = spark.sql(f"SHOW TABLES IN {database}")
    return tables.filter(tables.tableName == table_name).count() > 0

def create_data_table(key: str) -> None:
    info: dict = data_sources_dict[key]
    table_name = key.lower()
    ticker = info["ticker_symbol"]
    
    av_data = get_historical_ohlcv(ticker_symbol=ticker, output_size='full')
    df = pd.DataFrame.from_dict(av_data, orient="index")
    df = df.reset_index().rename(columns={'index': 'date'})

    spark_df = spark.createDataFrame(df)
    spark_df.write.saveAsTable(table_name)
    
    table_comment = info['notes']
    spark.sql(f"COMMENT ON TABLE {info['catalog_table_name']} IS '{table_comment}'")

    
def append_to_table(key: str) -> None:
    info: dict = data_sources_dict[key]
    table_name = key.lower()
    ticker = info["ticker_symbol"]

    df = spark.table(table_name)
    min_max_date = df.agg(min(DATE_COLUMN).alias('min_date'), max(DATE_COLUMN).alias('max_date')).collect()[0]
    max_date = min_max_date['max_date']
    min_date = min_max_date['min_date']

    date_difference = (max_date - min_date).days + 2
    start_date = max_date + timedelta(days=1)

    print(f"earliest date in table is: {min_date}")
    print(f"most recent date in table is: {max_date}")
    print(f"number of calendar days between earliest and most recent date in table (inclusive): {date_difference}")

    print(f"number of rows in table: {df.count()}")
    print(f"number of unique rows in table: {df.distinct().count()}")
    print(f"number of expected rows in table: {date_difference}")
    print(f"row count error: {date_difference - df.distinct().count()}")

    av_data = get_historical_ohlcv(ticker_symbol=ticker, output_size='compact')
    df = pd.DataFrame.from_dict(av_data, orient="index")
    df = df.reset_index().rename(columns={'index': 'date'})
    print("\n")

    spark_df = spark.createDataFrame(df)
    spark_df.write.mode("append").saveAsTable(info["catalog_table_name"])

# COMMAND ----------

for key in alpha_vantage_tickers.keys():
    info = alpha_vantage_tickers[key]
    ticker = info["ticker_symbol"]
    notes = info["notes"]
    table_name = key.lower()
    exists = table_exists(spark=spark, table_name=table_name, database=DATABASE_NAME)
    print(f"key: {key}, exists: {exists}")
    if not exists:
        create_data_table(key)
    else:
        append_to_table(key)

spy_data = get_historical_ohlcv("SPY")

# COMMAND ----------

# MAGIC %md
# MAGIC TODO:
# MAGIC 1. check for duplicates
# MAGIC 2. maybe interpolate days inbetween for monthly/quarterly data
