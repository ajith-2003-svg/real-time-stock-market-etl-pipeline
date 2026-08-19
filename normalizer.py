def normalize_bar(bar, source_type):
    return {
        "symbol": bar.symbol,
        "event_time": bar.timestamp.isoformat(),
        "open": float(bar.open),
        "high": float(bar.high),
        "low": float(bar.low),
        "close": float(bar.close),
        "volume": int(bar.volume),
        "trade_count": int(bar.trade_count) if bar.trade_count is not None else None,
        "vwap": float(bar.vwap) if bar.vwap is not None else None,
        "source": "alpaca",
        "source_type": source_type,
        "event_type": "bar"
    }

def normalize_trade(trade):
    return {
        "symbol": trade.symbol,
        "event_time": trade.timestamp.isoformat(),
        "price": float(trade.price),
        "size": float(trade.size),
        "source": "alpaca",
        "source_type": "live",
        "event_type": "trade"
    }