# Databricks notebook source
# MAGIC %config InteractiveShell.ast_node_interactivity='all'

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

## imports and local config
from pyspark.sql import SparkSession
import requests
import json
from datetime import datetime, timedelta
import pandas as pd

COINAPI_API_KEY = dbutils.secrets.get(scope="general", key="coinapi-api-key")

EXCHANGE = "COINBASE"
ASSET = "BTC"
BASE_CURRENCY = "USD"
TABLE_NAME = "btc_usd_daily_price"
DATA_SOURCE = f"https://rest.coinapi.io/v1/ohlcv/{EXCHANGE}_SPOT_{ASSET}_{BASE_CURRENCY}/history"

DATE_COLUMN = "date_period_start" # used for checking missing dates, earliest, latest dates in data

END_DATE = datetime.now().strftime("%Y-%m-%d")

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



# COMMAND ----------

from pyspark.sql.functions import col, count, datediff, row_number
from pyspark.sql.window import Window

if TABLE_EXISTS:
  #get existing date range
  df = spark.table(TABLE_NAME)

  # Calculate the expected number of rows based on the earliest and latest date
  min_max_date = df.agg(min(DATE_COLUMN).alias('min_date'), max(DATE_COLUMN).alias('max_date')).collect()[0]
  expected_rows = datediff(min_max_date['max_date'], min_max_date['min_date']) + 1

  max_date = min_max_date['max_date']
  min_date = min_max_date['min_date']

  # Calculate the total days between the earliest and most recent dates
  total_days = datediff(max_date, min_date)
  
  # Check if there's exactly one row per day
  window_spec = Window.partitionBy(DATE_COLUMN).orderBy(DATE_COLUMN)
  df = df.withColumn("row_num", row_number().over(window_spec))
  single_row_per_day = df.filter(df.row_num > 1).count() == 0
  
  if not single_row_per_day:
    raise ValueError("spark table contains more than 1 row per day. this makes no sense")
  
  print(f"earliest date in table is: {min_date}")
  print(f"most recent date in table is: {max_date}")
  START_DATE = max_date 
else:

  params = {
    'period_id': '1DAY',
    'time_start': START_DATE,
    'time_end': END_DATE,
  }
  headers = {
    'X-CoinAPI-Key': COINAPI_API_KEY
  }
  response = requests.get(DATA_SOURCE, headers=headers, params=params)

  if response.status_code != 200:
    raise ValueError(f"coinapi.io API returned bad status code: {response.status_code}")
  else:
    btc_data = json.loads(response.text)

# COMMAND ----------


# Convert to Spark DataFrame
df = pd.DataFrame(btc_data)
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

# CHECK THAT API DATA IS CONTINUOUS (no missing days).
# raises an error if there are missing days between earliest and latest days

DATAFRAME = df

def analyze_dates(df, date_column):
    """
    Analyze a date column in a DataFrame.
    
    Args:
    - df (pd.DataFrame): The input DataFrame.
    - date_column (str): The name of the date column to analyze.
    
    Returns:
    - tuple: Earliest date, most recent date, and a list of missing dates.
    """
    # Convert the column into a Pandas datetime object
    df[date_column] = pd.to_datetime(df[date_column])
    
    # Find the earliest and most recent dates
    earliest_date = df[date_column].min()
    most_recent_date = df[date_column].max()
    
    # Create a date range from the earliest to the most recent date
    full_date_range = pd.date_range(earliest_date, most_recent_date)
    
    # Find missing dates by comparing the date range with the unique dates in the date column
    missing_dates = full_date_range.difference(df[date_column].unique())
    
    return earliest_date, most_recent_date, missing_dates.tolist()

earliest, latest, missing = analyze_dates(DATAFRAME, DATE_COLUMN)

print(f"Earliest date: {earliest}")
print(f"Latest date: {latest}")

if missing:
    raise ValueError("There are dates missing inbetween earlist and latest dates")
else:
    print("**There are no missing dates between these limits**")


# COMMAND ----------

df.head()
df.dtypes

# COMMAND ----------

# Add the validated data to the datastore
from pyspark.sql.types import DateType, StringType

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


spark_df = spark.createDataFrame(df)
# Explicitly cast date-only columns to Spark's "date" datatype
for column in date_columns:
    spark_df = spark_df.withColumn(column, spark_df[column].cast(DateType()))
    
# Check if table exists and save
if spark._jsparkSession.catalog().tableExists(TABLE_NAME):
    spark_df.write.mode("append").saveAsTable(TABLE_NAME)
else:
    spark_df.write.saveAsTable(TABLE_NAME)

# COMMAND ----------


