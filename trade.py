# https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
# https://github.com/googleapis/python-aiplatform/blob/master/samples/snippets/predict_tabular_classification_sample.py
import config, json, websocket, pandas as pd
from binance.client import Client
from binance.enums import *
from datetime import datetime
from google.cloud import aiplatform
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value
from pprint import pprint


def model_predict(instance_dict):
    client_options = {"api_endpoint": f"{config.LOCATION}-aiplatform.googleapis.com"}
    client = aiplatform.gapic.PredictionServiceClient(client_options=client_options)
    instance = json_format.ParseDict(instance_dict, Value())
    instances = [instance]
    parameters_dict = {}
    parameters = json_format.ParseDict(parameters_dict, Value())
    endpoint = client.endpoint_path(project=config.TRADE_CONFIG["model"]["project"], location=config.LOCATION, endpoint=config.TRADE_CONFIG["model"]["endpoint_id"])
    response = client.predict(endpoint=endpoint, instances=instances, parameters=parameters)
    predictions = response.predictions
    return predictions[0]

def print_message(instance_dict):
    print('\nMessage start')
    print(f"Current time: {datetime.now()}")
    for key, value in instance_dict.items():
        instance_dict[key] = str(value)
        
        if key == 'open_time' or key == 'close_time':
            print(f"{key}: {datetime.fromtimestamp(int(value)/1000.0)}")
        else:
            print(f"{key}: {value}")
    print('Message end\n')
    return instance_dict
    
def get_instance_dict(candle):
    instance_dict = {
        'open_time': candle['t'],
        'open': candle['o'],
        'high': candle['h'],
        'low': candle['l'],
        'close': candle['c'],
        'volume': candle['v'],
        'close_time': candle['T'],
        'quote_asset_volume': candle['q'],
        'number_of_trades': candle['n'],
        'taker_buy_base_asset_volume': candle['V'],
        'taker_buy_quote_asset_volume': candle['Q'],
        'ignore': candle['B']
    }
    for key, value in instance_dict.items():
        instance_dict[key] = str(value)
    return instance_dict


def on_message(ws, message):
    data = json.loads(message)
    candle = data['k']
    closed = candle['x']
    instance_dict = get_instance_dict(candle)
    print_message(instance_dict)
    if closed:
        model_prediction = model_predict(instance_dict=instance_dict)

        prediction = dict(zip(model_prediction['classes'], model_prediction['scores']))

        if prediction['True'] > prediction['False']:
            side = SIDE_BUY
        elif prediction['False'] > prediction['True']:
            side = SIDE_SELL
        
        print(f"\nAttempting a {side} trade based on model prediction: {prediction}\n")
        
        try:
            client = Client(config.TRADE_CONFIG['api_key'], config.TRADE_CONFIG['api_secret'], tld='us')
            client.create_order(symbol=config.SYMBOL, side=side,type=ORDER_TYPE_MARKET, quantity=config.TRADE_CONFIG['amount'])
            print(f"{side} trade completed for {config.SYMBOL} in the amount of {config.TRADE_CONFIG['amount']}")
        except Exception as e:
            print(e)
            
            
def main():
    SOCKET_URL = f"wss://stream.binance.com:9443/ws/{config.SYMBOL.lower()}@kline_{config.DATA_CONFIG['interval']}"
    print(f"Starting websocket: {SOCKET_URL}")
    ws = websocket.WebSocketApp(SOCKET_URL, on_message=on_message)
    ws.run_forever()

main()