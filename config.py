# https://github.com/binance/binance-public-data/
LOCATION="us-central1" # vertex-ai is currently limited to us-central1 and europe-west4
SYMBOL="BTCUSDT"
DATA_CONFIG = {
    "start": "2021-05-01", # YYYY-MM-DD
    "end": "2021-05-31", # YYYY-MM-DD
    "interval": "1m", # See https://github.com/binance/binance-public-data/#intervals for intervals
    "bucket_name": "" # must be globally unique - https://cloud.google.com/storage/docs/naming-buckets
}

TRADE_CONFIG = {
    "api_key": "", # from binance
    "api_secret": "", # from binance
    "amount": 0.0,
    "model": {
        "project": "", # from gcp deployed model
        "endpoint_id": "" # from gcp deployed model
    }
}