import json
import boto3

from alpaca.data.live import StockDataStream

from config import (
    ALPACA_API_KEY,
    ALPACA_SECRET_KEY,
    AWS_REGION,
    KINESIS_STREAM_NAME,
    SYMBOLS
)

from normalizer import normalize_trade


kinesis = boto3.client(
    "kinesis",
    region_name=AWS_REGION
)

stream = StockDataStream(
    ALPACA_API_KEY,
    ALPACA_SECRET_KEY
)


async def trade_handler(trade):

    record = normalize_trade(trade)

    response = kinesis.put_record(
        StreamName=KINESIS_STREAM_NAME,
        Data=json.dumps(record),
        PartitionKey=record["symbol"]
    )

    print(
        f'{record["symbol"]} '
        f'Price: {record["price"]} '
        f'→ {response["ShardId"]}'
    )


stream.subscribe_trades(
    trade_handler,
    *SYMBOLS
)

print("Live stock producer started...")
print("Waiting for market events...")

stream.run()