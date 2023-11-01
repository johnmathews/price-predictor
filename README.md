# Price Predictor


A data engineering pipeline to demonstrate my skills using

- Azure databricks
- Azure DevOps
- DBT

## Goal 

An automated tool to load data from a variety of sources, transform it and build a model to predict
the price of bitcoin. 

## Data Sources

### Ideas

- Exchange order book data
- Sentiment data
- Financial data from traditional sources
    - Interest rates
        - ECB
        - Fed
        - BoE
    - Price of gold
    - Treasures
    - Inflation data
    - NASDAQ Index
    - Dow Jones
- Bitcoin derivatives, futures
    - CME
- Other crypto currencies such as Ethereum?
- On-chain metrics
    - mem pool size
    - average tnsx fee
    - miner hash rate

### Crypto data sources

#### CoinAPI.io

Data:

- historical
- Real-time

Limits:

- 100 reqs/day

#### Coinmarketcap

Real-time only on free tier

Data:

- pricing
- rankings
- market cap
- referential info
- exchange asset data

Limits:

- 10000/month
- 30 reqs/minute

## Tooling

- Azure DevOps
    - Build step
    - Deplot step
- Azure Databricks
    - Data lakehouse
        - Storage
    - SQL Warehouse - Compute
- DBT
    - documentation
    - testing
    - Model + DAG definitions

## UI

A tool this good should expose its results using some endpoints. Good thing databricks takes care of
that...

UI will be separate.
