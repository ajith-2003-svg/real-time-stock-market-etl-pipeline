import json
import boto3
from datetime import datetime

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

from config import (
    ALPACA_API_KEY,
    ALPACA_SECRET_KEY,
    AWS_REGION,
    KINESIS_STREAM_NAME,
    SYMBOLS
)

from normalizer import normalize_bar


alpaca_client = StockHistoricalDataClient(
    ALPACA_API_KEY,
    ALPACA_SECRET_KEY
)

kinesis = boto3.client(
    "kinesis",
    region_name=AWS_REGION
)

request = StockBarsRequest(
    symbol_or_symbols=SYMBOLS,
    timeframe=TimeFrame.Minute,
    start=datetime(2026, 8, 17, 15, 0),
    end=datetime(2026, 8, 17, 15, 10)
)

bars = alpaca_client.get_stock_bars(request)

count = 0

for symbol_bars in bars.data.values():

    for bar in symbol_bars:

        record = normalize_bar(
            bar,
            source_type="historical"
        )

        response = kinesis.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=json.dumps(record),
            PartitionKey=record["symbol"]
        )

        count += 1

        print(
            record["symbol"],
            response["ShardId"]
        )

print("Total records sent:", count)