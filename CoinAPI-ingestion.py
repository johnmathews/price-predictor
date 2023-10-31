# Databricks notebook source
import requests
import json
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# API Key (Stored as a Databricks secret for security)
coinapi_api_key = spark.conf.get("spark.databricks.secrets.general.coinapi-api-key")
coinmarketcap_api_key = spark.conf.get("spark.databricks.secrets.general.coinmarketcap-api-key")

# API Endpoint
url = "https://rest.coinapi.io/v1/ohlcv/BTC/USD/history?period_id=1DAY&time_start=2021-01-01T00:00:00"

# API Headers
headers = {
  'X-CoinAPI-Key': api_key
}

# Make the API call
response = requests.get(url, headers=headers)
data = json.loads(response.text)

# Convert to PySpark DataFrame
spark_df = spark.createDataFrame(data)

# Show DataFrame
spark_df.show()

