# Real-Time Stock Market Data Engineering Pipeline

An end-to-end real-time data engineering project that ingests live stock market trade data from Alpaca, streams it through AWS, processes it with Databricks and PySpark, loads curated data into Snowflake, and visualizes the results in Power BI.

The project was built as a hands-on implementation of a modern real-time ETL pipeline using cloud and big-data technologies.

---

## Architecture

```text
                    Alpaca Market Data API
                             |
                             v
                    Python Live Producer
                             |
                             v
                    AWS Kinesis Data Streams
                             |
                             v
                    Amazon Data Firehose
                             |
                             v
                       Amazon S3
                         Raw Layer
                             |
                             v
                  Databricks / PySpark
                    Auto Loader
                    Structured Streaming
                    Transformations
                             |
                             v
                       Amazon S3
                       Curated Layer
                             |
                             v
                    S3 Event Notification
                             |
                             v
                         AWS SNS
                             |
                             v
                         AWS SQS
                             |
                             v
                     Snowflake Snowpipe
                             |
                             v
                         Snowflake
                             |
                             v
                    Power BI DirectQuery
                             |
                             v
                  Stock Market Dashboard
```

---

## Technologies Used

- Python
- Alpaca Market Data API
- AWS Kinesis Data Streams
- Amazon Data Firehose
- Amazon S3
- Databricks
- Apache Spark / PySpark
- Databricks Auto Loader
- Structured Streaming
- Delta Lake
- Databricks Jobs
- AWS SNS
- AWS SQS
- Snowflake
- Snowpipe
- Power BI
- Git / GitHub

---

## Project Objective

The objective of this project is to demonstrate an end-to-end real-time data engineering pipeline.

Live stock trade events are captured from Alpaca and streamed into AWS Kinesis. Firehose automatically delivers the streaming records into an Amazon S3 raw data layer.

Databricks then incrementally processes newly arriving files using Auto Loader and PySpark. The data is cleaned, transformed, enriched, and written as Parquet files into a curated S3 layer.

New curated files trigger Snowflake Snowpipe through an SNS/SQS notification architecture. Snowpipe automatically loads the files into Snowflake tables.

Power BI connects to Snowflake using DirectQuery to provide analytical and near-real-time visualization of the processed stock market data.

---

## Data Sources

### Live Market Data

Live trade events are received through the Alpaca Stock Data WebSocket API.

Example trade record:

```json
{
  "symbol": "AAPL",
  "event_time": "2026-08-19T18:25:31.123456+00:00",
  "price": 225.42,
  "size": 100,
  "source": "alpaca",
  "source_type": "live",
  "event_type": "trade"
}
```

The live producer can subscribe to multiple stock symbols through the `SYMBOLS` configuration.

### Historical Market Data

A historical producer is also included for testing the pipeline when live market data is unavailable.

Historical bar records contain fields such as:

- Open
- High
- Low
- Close
- Volume
- Trade count
- VWAP

---

## Pipeline Flow

### 1. Data Ingestion

`live_producer.py` connects to the Alpaca live Stock Data WebSocket.

Each incoming trade is normalized and sent to AWS Kinesis Data Streams using `boto3`.

The stock symbol is used as the Kinesis partition key.

```text
Alpaca
   ↓
Python Producer
   ↓
Kinesis
```

---

### 2. Streaming Delivery

Amazon Data Firehose consumes records from Kinesis and automatically delivers them into the raw S3 bucket.

```text
Kinesis
   ↓
Firehose
   ↓
S3 Raw Layer
```

The raw layer preserves the original streaming data before transformation.

---

### 3. Incremental Processing with Databricks

Databricks Auto Loader monitors the raw S3 location for newly arriving JSON files.

Structured Streaming and checkpointing allow the pipeline to identify newly arrived files without repeatedly processing previously consumed files.

```text
Raw S3
   ↓
Auto Loader
   ↓
PySpark
   ↓
Bronze Delta
```

A Databricks continuous Job repeatedly executes the incremental processing notebook.

`Trigger.AvailableNow()` processes the currently available files and terminates the individual Spark query. The Databricks continuous Job automatically starts the next job run.

This provides automated incremental processing while using Databricks serverless compute.

---

## Data Transformations

PySpark transformations include:

### Trade Transformations

- Timestamp conversion
- Event date extraction
- Symbol standardization
- Duplicate removal
- Previous trade price
- Price change
- Percentage price change
- Five-record rolling average
- Trade size processing

Example calculations:

```text
Price Change
= Current Price - Previous Price
```

```text
Price Change %
= (Price Change / Previous Price) × 100
```

### Bar Transformations

Historical bar records include additional calculations:

```text
Price Range
= High - Low
```

```text
Bar Change
= Close - Open
```

```text
Bar Change %
= (Bar Change / Open) × 100
```

---

## Curated Data Layer

Processed data is separated into:

```text
Curated S3
│
├── trades/
│
└── bars/
```

The curated datasets are stored in Parquet format and partitioned by event date.

Parquet provides columnar storage suitable for analytical workloads.

---

## Snowflake Ingestion

Snowflake uses an external stage pointing to the curated S3 data.

Two Snowpipes handle the datasets:

```text
STOCK_TRADES_PIPE
STOCK_BARS_PIPE
```

The automatic ingestion flow is:

```text
New Parquet file
       ↓
Amazon S3
       ↓
SNS
       ↓
SQS
       ↓
Snowpipe
       ↓
Snowflake
```

This eliminates the need to manually execute `COPY INTO` whenever new curated files arrive.

---

## Snowflake Data Model

The curated Snowflake layer contains separate tables for trade and bar data.

### Trades

Important attributes include:

- SYMBOL
- EVENT_TIMESTAMP
- EVENT_DATE
- PRICE
- PREVIOUS_PRICE
- PRICE_CHANGE
- PRICE_CHANGE_PCT
- ROLLING_AVG_PRICE_5
- SIZE
- SOURCE
- SOURCE_TYPE

### Bars

Important attributes include:

- SYMBOL
- EVENT_TIMESTAMP
- EVENT_DATE
- OPEN
- HIGH
- LOW
- CLOSE
- PRICE_RANGE
- BAR_CHANGE
- BAR_CHANGE_PCT
- VOLUME
- TRADE_COUNT
- VWAP
- SOURCE
- SOURCE_TYPE

---

## Analytical Views

Snowflake views were created to simplify Power BI reporting.

Examples include:

```text
VW_LATEST_STOCK_PRICE
VW_TRADE_SUMMARY
VW_DAILY_STOCK_SUMMARY
VW_LIVE_PRICE_MOVEMENT
```

These views support metrics such as:

- Latest stock price
- Total trades
- Total trade size
- Average stock price
- Daily price movement
- Intraday price movement

---

## Power BI Dashboard

Power BI connects directly to Snowflake using **DirectQuery**.

The dashboard includes:

- Stock symbol slicer
- Latest price
- Total trades
- Total trade size
- Stock price movement
- Live/intraday price movement

Using DirectQuery allows Power BI to retrieve updated information from Snowflake without maintaining a separately imported copy of the dataset.

---

## Project Structure

```text
Stock_streaming_pipeline/
│
├── config.py
├── normalizer.py
├── live_producer.py
├── historical_producer.py
├── requirements.txt
├── README.md
├── .gitignore
└── .env
```

`.env` contains local credentials and is intentionally excluded from GitHub.

Databricks processing notebooks are maintained separately in the Databricks workspace and can also be exported into the repository for reference.

---

## Installation

Clone the repository:

```bash
git clone <repository-url>
cd Stock_streaming_pipeline
```

Create a virtual environment:

```bash
python -m venv .venv
```

Activate the environment.

Windows:

```bash
.venv\Scripts\activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Python Dependencies

`requirements.txt`

```text
alpaca-py
boto3
python-dotenv
```

---

## Environment Variables

Create a local `.env` file:

```text
ALPACA_API_KEY=your_api_key
ALPACA_SECRET_KEY=your_secret_key
```

AWS credentials should be configured using an appropriate AWS authentication method.

Never commit API keys, passwords, access keys, or other credentials to GitHub.

---

## Running the Live Pipeline

The downstream services and Databricks Job should be ready before starting the producer.

Run:

```bash
python live_producer.py
```

The resulting flow is:

```text
Live Alpaca Trades
        ↓
Kinesis
        ↓
Firehose
        ↓
Raw S3
        ↓
Databricks Continuous Job
        ↓
Curated S3
        ↓
SNS / SQS
        ↓
Snowpipe
        ↓
Snowflake
        ↓
Power BI
```

Stop the producer using:

```text
Ctrl + C
```

---

## Pipeline Validation

The pipeline was validated end-to-end using live market data.

Validation checkpoints included:

```text
Alpaca live events             ✓
Kinesis incoming records       ✓
Firehose delivery              ✓
Raw S3 objects                 ✓
Databricks automated runs      ✓
Curated Parquet files          ✓
Snowpipe ingestion             ✓
Snowflake records              ✓
Power BI dashboard             ✓
```

The final test confirmed that live trade records could travel from the Alpaca WebSocket through the complete pipeline and become available for reporting in Power BI.

---

## Cost Management

This project is intended as a learning and portfolio implementation rather than a continuously operating production system.

After testing, continuous processing resources should be stopped or paused to prevent unnecessary cloud charges.

Examples include:

- Stop the local live producer
- Pause the Databricks continuous Job
- Stop unnecessary Snowflake warehouse compute
- Disable or remove unused streaming infrastructure when the project is no longer required

---

## Future Improvements

A production implementation could be extended with:

- Stateful streaming calculations across micro-batches
- Data quality validation
- Dead-letter handling
- Schema evolution management
- Infrastructure as Code
- CI/CD deployment
- Automated monitoring and alerting
- Improved observability
- Secrets management
- More advanced Power BI analytics
- Additional market data sources

---

## Key Learning Outcomes

This project demonstrates practical experience with:

- Real-time data ingestion
- Event-driven architectures
- Streaming data pipelines
- AWS streaming services
- S3 data lake architecture
- PySpark transformations
- Structured Streaming
- Auto Loader
- Delta Lake
- Incremental processing
- Databricks Jobs
- Parquet data storage
- Snowflake external stages
- Snowpipe auto-ingestion
- SNS/SQS event notifications
- DirectQuery reporting
- End-to-end pipeline validation
- Cloud cost management

---

## Disclaimer

This project is intended for educational and portfolio purposes. Market data is used to demonstrate data engineering concepts and should not be interpreted as financial advice.