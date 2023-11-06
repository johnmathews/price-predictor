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
from datetime import datetime, timedelta
import pandas as pd

NASDAQ_API_KEY = dbutils.secrets.get(scope="general", key="nasdaq-api-key")
os.environ['NASDAQ_DATA_LINK_API_KEY'] = NASDAQ_API_KEY

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

# MAGIC %md
# MAGIC # Data Sources

# COMMAND ----------

# FRED_NROUST = nasdaqdatalink.get("FRED/NROUST", start_date=START_DATE, end_date=today)

# INDEX -  Nasdaq Daily Index Data & Analytics https://data.nasdaq.com/tables/NEID/NDAQ-EIV
# NDAQ_EIV = nasdaqdatalink.get("NDAQ/EIV", start_date=START_DATE, end_date=today)

# COMMAND ----------

data_sources_dict = {
    "ML_EMHYY": {
        "data_link_code": "ML/EMHYY",
        "catalog_table_name": "ML_EMHYY",
        "source_url": "https://data.nasdaq.com/data/ML/EMHYY-emerging-markets-high-yield-corporate-bond-index-yield",
        "notes": "BONDS - EM - High Yield - Emerging Markets High Yield Corporate Bond Index Yield",
    },
    "FRED_GDP": {
        "data_link_code": "FRED/GDP",
        "catalog_table_name": "FRED_GDP",
        "source_url": "",
        "notes": "",
    },
    "SP500_DIV_YIELD_MONTH": {
        "data_link_code": "MULTPL/SP500_DIV_YIELD_MONTH",
        "catalog_table_name":  "SP500_DIV_YIELD_MONTH",
        "source_url": "https://data.nasdaq.com/data/MULTPL/SP500_DIV_YIELD_MONTH-sp-500-dividend-yield-by-month",
        "notes": "S&P 500 Dividend Yield",
    },
    "ML_AAAEY": {
        "data_link_code": "ML/AAAEY",
        "catalog_table_name": "ML_AAAEY",
       "source_url": "https://data.nasdaq.com/data/ML/AAAEY-us-aaa-rated-bond-index-yield?",
        "notes": "BONDS INDEX - US AAA rated Bond Index (yield)",
    },
    "ML_AATRI": {
        "data_link_code": "ML/AATRI",
        "catalog_table_name": "ML_AATRI",
        "source_url": "https://data.nasdaq.com/data/ML/AATRI-us-aa-rated-total-return-index",
        "notes": "BONDS - US AA Rated Total Return Index ",
    },
    "ML_EMCTRI": {
        "data_link_code": "ML/EMCTRI",
        "catalog_table_name": "ML_EMCTRI",
        "source_url": "https://data.nasdaq.com/data/ML/EMCTRI-emerging-markets-corporate-bond-total-return-index",
        "notes": "BONDS - EM - Emerging Markets Corporate Bond Total Return Index ",
    },
    "ML_USEY": {
        "data_link_code": "ML/USEY",
        "catalog_table_name": "ML_USEY",
        "source_url": "https://data.nasdaq.com/data/ML/USEY-us-corporate-bond-index-yield",
        "notes": "BONDS - US Corporate - US Corporate Bond Index Yield",
    },
      "FRED_NROUST": {
        "data_link_code": "FRED/NROUST",
        "catalog_table_name": "FRED_NROUST",
        "source_url": "https://data.nasdaq.com/data/FRED/NROUST-natural-rate-of-unemployment-shortterm",
        "notes": "STAT - US Natural Rate of Unemployment (Short-Term)",
    },
    #"demo": {
    #    "data_link_code": "demo",
    #    "catalog_table_name": "demo",
    #    "source_url": "demo",
    #    "notes": "demo",
    #},
}

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
    df = nasdaqdatalink.get(info['data_link_code'], start_date=START_DATE, end_date=today)
    df = df.reset_index()
    print(f"columns: {df.columns}")
    spark_df = spark.createDataFrame(df)
    spark_df.write.saveAsTable(info["catalog_table_name"])
    table_comment = info['notes'] + " - URL: " + info['source_url']
    print(f"table comment: {table_comment}\n")
    spark.sql(f"COMMENT ON TABLE {info['catalog_table_name']} IS '{table_comment}'")

    
def append_to_table(key: str) -> None:
    info: dict = data_sources_dict[key]
    table_name = info["catalog_table_name"]

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

    df = nasdaqdatalink.get(info['data_link_code'], start_date=start_date, end_date=today)
    df = df.reset_index()
    print(f"columns: {df.columns}")
    print("\n")

    if not df.empty:
        spark_df = spark.createDataFrame(df)
        spark_df.write.mode("append").saveAsTable(info["catalog_table_name"])

# COMMAND ----------

for key in data_sources_dict.keys():
    item = data_sources_dict[key]
    table_name=item['catalog_table_name'].lower()
    print(f"{table_name = }")
    exists = table_exists(spark=spark, table_name=table_name, database=DATABASE_NAME)
    print(f"key: {key}, exists: {exists}")
    if not exists:
        create_data_table(key)
    else:
        append_to_table(key)


# COMMAND ----------

# MAGIC %md
# MAGIC TODO:
# MAGIC 1. check for duplicates
# MAGIC 2. maybe interpolate days inbetween for monthly/quarterly data
