# Price Predictor


A data engineering pipeline to demonstrate my skills using

- Azure databricks
- Azure DevOps
- DBT

## Goal 

An automated tool to load data from a variety of sources, transform it and build a model to predict
the price of bitcoin. 

## Data Sources

1. BTC - daily OHLCV
2. ETH - daily OHLCV
3. CAC40 - daily - Alpha Vantage - Lyxor CAC 40 ETF (traded on Euronext Paris)
4. DAX - daily - Alpha Vantage - Global X DAX Germany ETF
5. DIA - daily - Alpha Vantage - SPDR Dow Jones Industrial Average ETF
6. USA GCP - FRED -quarterly
7. NROUST - quarterly - USA unemployment short term - STAT - US Natural Rate of Unemployment (Short-Term) 
8. FTSE100 - daily - OHLCV - Alpha Vantage - iShares Core FTSE 100 UCITS ETF (traded on the London Stock Exchange)
9. ML_AAAEY - daily - BONDS INDEX - NASDAQ DATA LINK - US AAA rated Bond Index (yield) 
10. ML_AATRI - daily - BONDS - US AA Rated Total Return Index 
11. ML_EMCTRI - daily - BONDS - EM - Emerging Markets Corporate Bond Total Return Index 
12. ML_EMHYY - daily - BONDS - EM - High Yield - Emerging Markets High Yield Corporate Bond Index Yield
13. ML_USEY - daily - BONDS - US Corporate - US Corporate Bond Index Yield
14. NIKKEI225 - daily OHLCV - Alpha Vantage - iShares MSCI Japan ETF (tracks a broad range of
    Japanese stocks)
15. 



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
