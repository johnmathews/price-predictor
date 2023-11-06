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


class UnnecessaryAPICallError(Exception):
    """Exception raised when an API call is made but is unnecessary due to business logic."""
    def __init__(self, message="The API call was unnecessary and has been prevented"):
        self.message = message
        super().__init__(self.message)

config = read_ini_config_from_workspace(CONFIG_PATH)
START_DATE = get_config_value(config, "General", "start_date")
DATABASE_NAME = get_config_value(config, "General", "database_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook constants

# COMMAND ----------

## imports and local config
from pyspark.sql import SparkSession
import requests
import json
from datetime import datetime, timedelta
import pandas as pd

COINAPI_API_KEY = dbutils.secrets.get(scope="general", key="coinapi-api-key")
DAYS_WITH_MISSING_DATA = 10 # apparently coinAPI.io doesnt have data for 10 days

EXCHANGE = "COINBASE"
ASSET = "BTC"
BASE_CURRENCY = "USD"
TABLE_NAME = "btc_usd_daily_price"
ENDPOINT = f"https://rest.coinapi.io/v1/ohlcv/{EXCHANGE}_SPOT_{ASSET}_{BASE_CURRENCY}/history"

DATE_COLUMN = "date_period_start" # used for checking missing dates, earliest, latest dates in data

END_DATE = datetime.now().strftime("%Y-%m-%d")

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
    tables = spark.sql(f"SHOW TABLES IN {database}")
    return tables.filter(tables.tableName == table_name).count() > 0

TABLE_EXISTS = True if table_exists(spark, TABLE_NAME, DATABASE_NAME) else False
print(f"{TABLE_NAME} exists: {TABLE_EXISTS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### If the table already exists, update START_DATE to reflect data already in table

# COMMAND ----------

from pyspark.sql.functions import col, count, datediff, row_number, min, max, date_format
from pyspark.sql.window import Window

if TABLE_EXISTS:
  #get existing date range
  df = spark.table(TABLE_NAME)

  # Calculate the expected number of rows based on the earliest and latest date
  min_max_date = df.agg(min(DATE_COLUMN).alias('min_date'), max(DATE_COLUMN).alias('max_date')).collect()[0]

  max_date = min_max_date['max_date']
  min_date = min_max_date['min_date']
  date_difference = (max_date - min_date).days + 2

  START_DATE = max_date + timedelta(days=1)

  print(f"earliest date in table is: {min_date}")
  print(f"most recent date in table is: {max_date}")
  print(f"number of calendar days between earliest and most recent date in table (inclusive): {date_difference}")
  print(f"\n*** CoinAPI.io is missing data for {DAYS_WITH_MISSING_DATA} days ***\n")
  print(f"number of rows in table: {df.count()}")
  print(f"number of unique rows in table: {df.distinct().count()}")
  print(f"number of expected rows in table: {date_difference - DAYS_WITH_MISSING_DATA}")
  print(f"row count error: {date_difference - DAYS_WITH_MISSING_DATA - df.distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get data using coinapi.io REST API

# COMMAND ----------

def coinapi_request(ENDPOINT, START_DATE, END_DATE, API_KEY):
    if START_DATE == datetime.now().date():
        raise UnnecessaryAPICallError("start date is today! no data will be returned from API. ")

    params = {
    'period_id': '1DAY',
    'time_start': START_DATE,
    'time_end': END_DATE,
    }

    headers = {
    'X-CoinAPI-Key': COINAPI_API_KEY
    }

    response = requests.get(ENDPOINT, headers=headers, params=params)

    if response.status_code != 200:
        print(f"response: {response.text}")
        raise HTTPError(f"coinapi.io API returned bad status code: {response.status_code}")
    else:
        print(f"response status: {response.status_code}")

    json_data = json.loads(response.text)
    return pd.DataFrame(json_data)

# COMMAND ----------

df = coinapi_request(ENDPOINT, START_DATE, END_DATE, COINAPI_API_KEY)

# COMMAND ----------


if START_DATE == datetime.now().date():
    raise UnnecessaryAPICallError("start date is today! no data will be returned from API. ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## convert API response (JSON) to to pandas dataframe. 
# MAGIC ### and shape the data - split datetime to data and time, etc

# COMMAND ----------

# Convert to Spark DataFrame
print(f"column names from API provider: {df.columns.tolist()}")

df['time_period_start'] = pd.to_datetime(df['time_period_start'])
df['time_period_end'] = pd.to_datetime(df['time_period_end'])
df['time_open'] = pd.to_datetime(df['time_open'])
df['time_close'] = pd.to_datetime(df['time_close'])
    
# Extract date and time components
df['date_period_start'] = df['time_period_start'].dt.date
df['time_period_start'] = df['time_period_start'].dt.time.astype(str)

df['date_period_end'] = df['time_period_end'].dt.date
df['time_period_end'] = df['time_period_end'].dt.time.astype(str)

df['date_open'] = df['time_open'].dt.date
df['time_open'] = df['time_open'].dt.time.astype(str)

df['date_close'] = df['time_close'].dt.date
df['time_close'] = df['time_close'].dt.time.astype(str)

cols = [
    "date_period_start",
    "time_period_start",
    "date_period_end",
    "time_period_end",
    "date_open",
    "time_open",
    "date_close",
    "time_close",
    "price_open",
    "price_close",
    "price_high",
    "price_low",
    "volume_traded",
    "trades_count",
]
df = df[cols]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append new data to spark table, or create a table if it doesnt already exist
# MAGIC ### Explicitly cast columns to correct datatype

# COMMAND ----------

# Add the validated data to the datastore
from pyspark.sql.types import DateType, StringType, LongType, DoubleType

date_columns = [
    "date_period_start",
    "date_period_end",
    "date_open",
    "date_close",
]

time_columns = [
    "time_period_start",
    "time_period_end",
    "time_open",
    "time_close",
]

doubleType_columns = [
    "price_low",
    "price_high",
    "price_open",
    "price_close",
    "volume_traded",
]

longType_columns = [
    "trades_count",
]

spark_df = spark.createDataFrame(df)

for column in doubleType_columns:
    spark_df = spark_df.withColumn(column, spark_df[column].cast(DoubleType()))

for column in longType_columns:
    spark_df = spark_df.withColumn(column, spark_df[column].cast(LongType()))

for column in date_columns:
    spark_df = spark_df.withColumn(column, spark_df[column].cast(DateType()))
    
if spark._jsparkSession.catalog().tableExists(TABLE_NAME):
    spark_df.write.mode("append").saveAsTable(TABLE_NAME)
else:
    spark_df.write.saveAsTable(TABLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make a function to check that there is exactly one row for each day between the earliest and most recent dates, 
# MAGIC ### no duplicates, no misses

# COMMAND ----------

# CHECK THAT API DATA IS CONTINUOUS (no missing days).
# raises an error if there are missing days between earliest and latest days

def analyze_dates(df, date_column):
    """
    Analyze a date column in a DataFrame to find the earliest, latest, missing, and duplicated dates.
    
    Args:
    - dataframe (pd.DataFrame): The input DataFrame.
    - date_column (str): The name of the date column to analyze.
    
    Returns:
    - tuple: Earliest date, most recent date, list of missing dates, and list of duplicated dates.
    """

    print(f"date_column: {date_column}")
    # Ensure date_column is a datetime object
    df[date_column] = pd.to_datetime(df[date_column].to_numpy(), errors="coerce")
    
    # Find the earliest and most recent dates
    earliest_date = df[date_column].min()
    most_recent_date = df[date_column].max()
    
    # Create a date range from the earliest to the most recent date
    full_date_range = pd.date_range(earliest_date, most_recent_date)
    
    # Identify missing dates by comparing the full date range with the unique dates
    missing_dates = full_date_range.difference(df[date_column].unique()).tolist()
    
    # Find duplicated dates by filtering the date column
    duplicated_dates = df[df.duplicated(subset=date_column, keep=False)][date_column]
    duplicated_dates = sorted(duplicated_dates.unique())
    
    return earliest_date, most_recent_date, missing_dates, duplicated_dates


# COMMAND ----------

# MAGIC %md
# MAGIC ## Housekeeping
# MAGIC ### remove duplicates
# MAGIC why are there duplicates? are you requesting data you already have?

# COMMAND ----------

df = spark.table(TABLE_NAME).toPandas()
duplicate_count = df.duplicated(keep='first').sum()
print(f"number of duplicates in table: {duplicate_count}")

if duplicate_count:
    df_unique = df.drop_duplicates(keep='first')
    spark_df = spark.createDataFrame(df_unique)

    spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    spark_df.write.saveAsTable(TABLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the stored Spark table for missing dates and duplicated dates
# MAGIC

# COMMAND ----------

df = spark.table(TABLE_NAME).toPandas()
earliest, latest, missing_dates, duplicated_dates = analyze_dates(df, DATE_COLUMN)

print(f"Earliest date: {earliest}")
print(f"Latest date: {latest}")

if missing_dates:
    print(f"Missing dates:")
    for d in missing_dates:
        print(d)
else:
    print("**There are no missing dates between these limits**")


if duplicated_dates:
    print("Duplicated dates:")
    for d in duplicated_dates:
        print(d)
else:
    print("**There are no duplicated dates between these limits**")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Looks like missing data is missing on CoinAPI.io data..

# COMMAND ----------

for d in missing_dates:
    print(f"d: {d.strftime('%Y-%m-%d')}")
    start = (d + timedelta(days=0)).strftime("%Y-%m-%d")
    end = (d + timedelta(days=0)).strftime("%Y-%m-%d")
    print(f"start: {start}")
    print(f"end: {end}")
    df = coinapi_request(ENDPOINT, start, end, COINAPI_API_KEY)
    df.head()
    print("\n")

# COMMAND ----------


