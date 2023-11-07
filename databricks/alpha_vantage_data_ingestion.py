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
# MAGIC ## Alpha Vantage API retrieval

# COMMAND ----------

def get_historical_ohlcv(ticker_symbol: str, output_size: str):
    """
    Retrieve historical open, high, low, close, and volume (OHLCV) data for a given ticker symbol.

    This function queries the Alpha Vantage API for daily OHLCV data. It allows the user to specify 
    whether to receive a compact dataset of the most recent 100 data points or a full dataset 
    that includes up to 20 years of historical data. Adjusted close values are not included by default.
    
    Parameters:
    - ticker_symbol (str): The ticker symbol of the stock to retrieve data for.
    - output_size (str): The size of the dataset to return ('compact' for 100 data points, 'full' for up to 20 years).
    
    Returns:
    - dict: A dictionary where each key is a date (YYYY-MM-DD) and each value is a dictionary of OHLCV data.
    
    Raises:
    - ValueError: If the API query returns an error, a ValueError is raised with the error message.

    Note: This function requires an API key for Alpha Vantage stored in ALPHA_VANTAGE_API_KEY.
    """

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
    "CORN": {
        "type": "function",
        "ticker_symbol": "CORN",
        "interval": "monthly",
        "notes": "Alpha Vantage -  the global price of corn in monthly, quarterly, and annual horizons. By default, interval=monthly. Strings monthly, quarterly, and annual are accepted.",
    },
    "COFFEE": {
        "type": "function",
        "ticker_symbol": "COFFEE",
        "interval": "monthly",
        "notes": "Alpha Vantage - global price of coffee in monthly, quarterly, and annual horizons. By default, interval=monthly. Strings monthly, quarterly, and annual are accepted.",
    },
    "SUGAR": {
        "type": "function",
        "ticker_symbol": "SUGAR",
        "interval": "monthly",
        "notes": "Alpha Vantage - s the global price of sugar in monthly, quarterly, and annual horizons. By default, interval=monthly. Strings monthly, quarterly, and annual are accepted." ,
    },
    "ALUMINUM": {
        "type": "function",
        "ticker_symbol": "ALUMINUM",
        "interval": "monthly",
        "notes": "Alpha Vantage -  global price of aluminum in monthly, quarterly, and annual horizons. By default, interval=monthly. Strings monthly, quarterly, and annual are accepted.",
    },
    "COPPER": {
        "type": "function",
        "ticker_symbol": "COPPER",
        "interval": "monthly",
        "notes": "Alpha Vantage - global price of copper in monthly, quarterly, and annual horizons. By default, interval=monthly. Strings monthly, quarterly, and annual are accepted.",
    },
    "NATURAL_GAS": {
        "type": "function",
        "ticker_symbol": "NATURAL_GAS",
        "interval": "daily",
        "notes": "Alpha Vantage -  the Henry Hub natural gas spot prices in daily, weekly, and monthly horizons. By default, interval=monthly. Strings daily, weekly, and monthly are accepted.",
    },
    "WHEAT": {
        "type": "function",
        "ticker_symbol": "WHEAT",
        "interval": "monthly",
        "notes": "Alpha Vantage - global price of wheat in monthly, quarterly, and annual horizons. By default, interval=monthly. Strings monthly, quarterly, and annual are accepted.",
    },
    "WTI": {
        "type": "function",
        "ticker_symbol": "WTI",
        "interval": "monthly",
        "notes": "Alpha Vantage - West Texas Intermediate (WTI) crude oil prices in daily, weekly, and monthly horizons. By default, interval=monthly. Strings daily, weekly, and monthly are accepted.",
    },
    "BRENT": {
        "type": "function",
        "ticker_symbol": "BRENT",
        "interval": "daily",
        "notes": "Alpha Vantage - Brent (Europe) crude oil prices in daily, weekly, and monthly horizons. By default, interval=monthly. Strings daily, weekly, and monthly are accepted.",
    },
    "TREASURY_YIELD": {
        "type": "function",
        "ticker_symbol": "TREASURY_YIELD",
        "notes": "Alpha Vantage - daily, weekly, and monthly US treasury yield of a given maturity timeline (e.g., 5 year, 30 year, etc). By default, maturity=10year. Strings 3month, 2year, 5year, 7year, 10year, and 30year are accepted.",
    },
    "USA_INTEREST_RATE": {
        "type": "function",
        "ticker_symbol": "FEDERAL_FUNDS_RATE",
        "notes": "Alpha Vantage - daily, weekly, and monthly federal funds rate (interest rate) of the United States.",
    },
    "CPI": {
        "type": "function",
        "ticker_symbol": "CPI",
        "notes": "Alpha Vantage - monthly and semiannual consumer price index (CPI) of the United States. CPI is widely regarded as the barometer of inflation levels in the broader economy.",
    },
    "INFLATION": {
        "type": "function",
        "ticker_symbol": "INFLATION",
        "notes": "Alpha Vantage - annual inflation rates (consumer prices) of the United States.",
    },
    "ADVANCED_RETAIL_SALE": {
        "type": "function",
        "ticker_symbol": "RETAIL_SALE",
        "notes": "Alpha Vantage - monthly Advance Retail Sales: Retail Trade data of the United States. Source: U.S. Census Bureau, Advance Retail Sales: Retail Trade, retrieved from FRED, Federal Reserve Bank of St. Louis",
    },
    "DURABLES": {
        "ticker_symbol": "DURABLES",
        "type": "function",
        "notes": "Alpha Vantage - monthly manufacturers' new orders of durable goods in the United States.",
    },
    "UNEMPLOYMENT": {
        "ticker_symbol": "UNEMPLOYMENT",
        "type": "function",
        "notes": "Alpha Vantage - monthly unemployment data of the United States. The unemployment rate represents the number of unemployed as a percentage of the labor force.",
    },
    "NONFARM_PAYROLL": {
        "ticker_symbol": "NONFARM_PAYROLL",
        "type": "function",
        "notes": "Alpha Vantage - monthly US All Employees: Total Nonfarm (commonly known as Total Nonfarm Payroll), a measure of the number of U.S. workers in the economy that excludes proprietors, private household employees, unpaid volunteers, farm employees, and the unincorporated self-employed.",
    },
    "SPY": {
        "ticker_symbol": "SPY",
        "notes": "Alpha Vantage - SPDR S&P 500 ETF Trust",
        "type": "time_series_daily",
    },
    "QQQ": {
        "ticker_symbol": "QQQ",
        "type": "time_series_daily",
        "notes": "Alpha Vantage - Invesco QQQ Trust, which tracks the NASDAQ-100, a subset of the NASDAQ Composite",
    },
    "DIA": {
        "ticker_symbol": "DIA",
        "type": "time_series_daily",
        "notes": "Alpha Vantage - SPDR Dow Jones Industrial Average ETF",
    },
    "FTSE100": {
        "ticker_symbol": "ISF.L",
        "type": "time_series_daily",
        "notes": "Alpha Vantage - iShares Core FTSE 100 UCITS ETF (traded on the London Stock Exchange)",
    },
    "Nikkei225": {
        "ticker_symbol": "EWJ",
        "type": "time_series_daily",
        "notes": "Alpha Vantage - iShares MSCI Japan ETF (tracks a broad range of Japanese stocks)",
    },
    "DAX": {
        "ticker_symbol": "DAX",
        "type": "partime_series_dailyams",
        "notes": "Alpha Vantage -  Global X DAX Germany ETF",
    },
    "CAC40": {
        "ticker_symbol": "CAC",
        "type": "time_series_daily",
        "notes": "Alpha Vantage - Lyxor CAC 40 ETF (traded on Euronext Paris)",
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark setup

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility functions

# COMMAND ----------

def clean_column_names(df):
    """
    Removes spaces from all column names in the DataFrame and replaces bad characters with underscores.
    """
    # Define bad characters which you want to replace with an underscore
    bad_chars = [' ', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '+', '=', '{', '}', '[', ']', '|', '\\', ':', ';', '"', "'", '<', '>', ',', '.', '?', '/']

    # Replace spaces and bad characters in column names
    df.columns = df.columns.str.translate(str.maketrans({char: '_' for char in bad_chars}))
    
    return df


def clean_dataframe(df, date_column, cutoff_date):
    """
    This function takes a DataFrame, the name of the date column, and a cutoff date.
    It converts the date column to datetime, sorts the DataFrame, and removes
    any rows with dates before the cutoff date.
    """
    # Convert the date column to datetime if not already in datetime format
    df[date_column] = pd.to_datetime(df[date_column])

    # Sort the DataFrame by the date column
    df.sort_values(by=date_column, inplace=True)

    # Remove rows with dates before the given cutoff date
    filtered_df = df[df[date_column] > pd.to_datetime(cutoff_date)]
    
    if not filtered_df.empty: 
        min_date = filtered_df[date_column].min()
        max_date = filtered_df[date_column].max()
        print(f"The oldest date is: {min_date}")
        print(f"The most recent date is: {max_date}")
    else:
        print(f"no new rows to append")

    return filtered_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get and process data

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
    info: dict = alpha_vantage_tickers[key]
    table_name = key.lower()
    ticker = info["ticker_symbol"]
    
    av_data = get_historical_ohlcv(ticker_symbol=ticker, output_size='full')
    df = pd.DataFrame.from_dict(av_data, orient="index")
    df = df.reset_index().rename(columns={'index': 'date'})
    df['date'] = pd.to_datetime(df['date'])
    df = clean_column_names(df)

    spark_df = spark.createDataFrame(df)
    spark_df.write.saveAsTable(table_name)
    
    table_comment = info['notes']
    spark.sql(f"COMMENT ON TABLE {key.lower()} IS '{table_comment}'")



    
def append_to_table(key: str, method: str = ) -> None:
    info: dict = alpha_vantage_tickers[key]
    table_name = key.lower()
    ticker = info["ticker_symbol"]

    df = spark.table(table_name)
    min_max_date = df.agg(min('date').alias('min_date'), max('date').alias('max_date')).collect()[0]
    max_date = min_max_date['max_date']
    min_date = min_max_date['min_date']
    
    #max_date = datetime.strptime(min_max_date['max_date'], "%Y-%m-%d")
    #min_date = datetime.strptime(min_max_date['min_date'], "%Y-%m-%d")
    
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
    df = clean_dataframe(df, date_column="date", cutoff_date=max_date)
    
    if not df.empty:
        df = clean_column_names(df)

        spark_df = spark.createDataFrame(df)
        spark_df.write.mode("append").saveAsTable(info["catalog_table_name"])
    
    print("\n")

# COMMAND ----------

for key in alpha_vantage_tickers.keys():
    info = alpha_vantage_tickers[key]
    ticker = info["ticker_symbol"]
    notes = info["notes"]
    table_name = key.lower()
    exists = table_exists(spark=spark, table_name=table_name, database=DATABASE_NAME)
    print(f"key: {key}, exists: {exists}")
    
    if info['type'] != 'time_series_daily':
        pass

    if not exists:
        create_data_table(key)
    else:
        append_to_table(key)

# COMMAND ----------

# MAGIC %md
# MAGIC TODO:
# MAGIC 1. check for duplicates
# MAGIC 2. maybe interpolate days inbetween for monthly/quarterly data

# COMMAND ----------

dbutils.notebook.exit("SUCCESS - all cells were run, none were skipped.")
