'''
for each partition:
1. download the partition (unzip to csv)
2. adjust the column headers
3. resave the csv to parsed folder
4. move entire folder from server to cloud storage
'''

import config, wget, zipfile, os
import pandas as pd
from google.cloud import storage
from datetime import datetime

def get_or_create_directory(directory):
    if os.path.exists(directory) == False:
        os.mkdir(directory)
    return directory

def get_or_create_bucket(bucket_name=config.DATA_CONFIG["bucket_name"]):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    if bucket.exists() == False:
        client.create_bucket(bucket, location=config.LOCATION)
    return client.get_bucket(bucket_name)

def upload_data(filepath, filename):
    bucket = get_or_create_bucket(bucket_name=config.DATA_CONFIG["bucket_name"])
    blob = bucket.blob(filename)
    blob.upload_from_filename(f"{filepath}/{filename}")

def get_date_range(start, end):
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")
    date_range = pd.date_range(start_date, end_date)
    return date_range

def get_data(start, end):
    symbol = config.SYMBOL
    interval = config.DATA_CONFIG['interval']
    date_range = get_date_range(start=start, end=end)
    directory = get_or_create_directory('raw_data')
    for date in date_range:
        date_str = datetime.strftime(date, "%Y-%m-%d")
        filename = f"{symbol}-{interval}-{date_str}.zip"
        download_path = f"https://data.binance.vision/data/spot/daily/klines/{symbol}/{interval}/{filename}"
        wget.download(download_path, f"{directory}/{filename}")
    
        with zipfile.ZipFile(f"{directory}/{filename}", 'r') as zip_ref:
            zip_ref.extractall(directory)

def prepare_data(start, end):
    directory = get_or_create_directory('raw_data')
    date_range = get_date_range(start=start, end=end)
    columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
    df = pd.DataFrame(columns=columns)
    for date in date_range:
        date_str = datetime.strftime(date, "%Y-%m-%d")
        filename = f"{config.SYMBOL}-{config.DATA_CONFIG['interval']}-{date_str}.csv"
        partition_df = pd.read_csv(f"{directory}/{filename}", names=columns, header=None)
        df = df.append(partition_df)

    df['next_close'] = df['close'].shift(-1)
    df['target'] = df['close'] < df['next_close']
    directory = get_or_create_directory('output_data')
    output_file = f"{directory}/{config.SYMBOL}_{start}_{end}_consolidated.csv"
    df[columns+['target']].to_csv(output_file, index=None)
    upload_data(filepath=directory, filename=f"{config.SYMBOL}_{start}_{end}_consolidated.csv")

def main():
    start = config.DATA_CONFIG["start"]
    end = config.DATA_CONFIG["end"]
    get_data(start=start, end=end)
    prepare_data(start=start, end=end)

main()
