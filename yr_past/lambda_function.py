import os
import requests
import datetime as dt
import json
import boto3
import math
from dotenv import load_dotenv
import pandas as pd
import numpy as np


load_dotenv()

TIMESTREAM_DATATYPES = ("DOUBLE", "BIGINT", "VARCHAR", "BOOLEAN")

DATABASE_NAME = os.getenv("DATABASE_NAME")
TABLE_NAME = os.getenv("TABLE_NAME")
CLIENT_ID = os.getenv("CLIENT_ID")

assert DATABASE_NAME
assert TABLE_NAME
assert CLIENT_ID

def get_measurement(data: pd.Series):
    measurements = []
    for name in data.index:
        if name != "max_wind_speed(wind_from_direction PT1H)":
            if data.name != np.nan:
                measurements.append({
                    "Name": name,
                    "Value": str(data[name]),
                    "Type": "DOUBLE"
                })
    if "max_wind_speed(wind_from_direction PT1H)" in data.index:
        # wind direction is set together of sin and cos to avoid 359 -> 0 
        wind_direction = float(data["max_wind_speed(wind_from_direction PT1H)"])
        if wind_direction != np.nan:
            cos_val = math.cos(math.radians(wind_direction))
            sin_val = math.sin(math.radians(wind_direction))
            measurements.append({
                "Name": f"wind_direction_cos",
                "Value": str(cos_val),
                "Type": "DOUBLE"
            })
            measurements.append({
                "Name": f"wind_direction_sin",
                "Value": str(sin_val),
                "Type": "DOUBLE"
            })
    return measurements

def get_and_extract_yr_data(lat: float, lon: float, gateway_id: int, measure_station: str):
    """Gets the historical data from yr.no by using the FROST API,
    and returns the rows to be inserted to the timestream database

    :param lat: Lat of the gateway, to be stored in the database. Future usage: lat for dynamically getting the source-id of the met-station
    :param lon: Lon of the gateway, to be stored in the database. Future usage: lon for dynamically getting the source-id of the met-station
    :param gateway_id: Gateway id to be stored in the database
    :param measure_station: id of met-station. Can be found via the FROST API at https://frost.met.no/sources/v0.jsonld
    :return: rows to be inserted into database
    """
    client_id = CLIENT_ID
    # api endpoint for getting the sources if that is needed
    # endpoint = f"https://frost.met.no/sources/v0.jsonld?geometry=nearest(POINT({round(lon, 4)} {round(lat, 4)}))"
    
    # get data from yesterday and today. Is set to tomorrow at 00:00. 
    # This is done to avoid gaps at 23-00 
    tomorrow = dt.date.today() + dt.timedelta(days=1)
    yesterday = dt.date.today() - dt.timedelta(days=1)
    endpoint = 'https://frost.met.no/observations/v0.jsonld'
    parameters = {
        'sources': measure_station,
        'elements': 'sum(precipitation_amount PT1H),max(air_temperature PT1H),max(wind_speed PT1H),max_wind_speed(wind_from_direction PT1H), ',
        'referencetime': f'{yesterday.isoformat()}/{tomorrow.isoformat()}',
    }
    r = requests.get(endpoint, parameters, auth=(client_id,''))
    json_data = r.json()
    data = json_data['data']

    # get all observations and the time
    df = pd.json_normalize(data, record_path=['observations'], meta='referenceTime')
    df = df[['referenceTime','value', 'elementId']].rename(columns={'referenceTime':'timestamp'})
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # reset table structure so that the columns is the values of the sensors and the
    # index is the timestamp.
    df = df.pivot(index='timestamp', columns='elementId', values='value').reset_index()
    df.set_index('timestamp', inplace=True)

    # add all columns as columns in the timestream database
    records = []
    measure_values = []
    for timestamp, row in df.iterrows():
        measure_values = get_measurement(data=row)
        record = {
            'Dimensions': [
                {'Name': 'lat', 'Value': str(lat)},
                {'Name': 'lon', 'Value': str(lon)},
                {'Name': 'gateway_id', 'Value': str(gateway_id)},

            ],
            'Time': str(int(timestamp.timestamp()*1000)),
            'MeasureName': 'yr_past',
            'MeasureValueType': 'MULTI',
            'MeasureValues': measure_values
        }
        if len(measure_values) != 0:
            records.append(record)
    # print(record)
    return records


def fetch_and_insert_sensor_data(names=None):
    # load positions to get yr data from
    positions = open("yr_position.json")
    schema = json.load(positions)
    records = []

    for point in schema:
        # for cli usage for one or multiple points
        if names is not None and point['name'] not in names:
            continue
        else:
            records += get_and_extract_yr_data(
                lat=point['lat'], 
                lon=point['lon'], 
                gateway_id=point['gatewayId'], 
                measure_station=point['measureStation'])
    if len(records) == 0:
        raise ValueError("The specified ids has no matching schemas.")

    client = boto3.client("timestream-write")
    for window_i in range(0, len(records), 100):
        record_window = records[window_i: window_i + 100]
        try:
            result = client.write_records(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME,
                                                Records=record_window, CommonAttributes={'Version': round(dt.datetime.now().timestamp())})
            print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except client.exceptions.RejectedRecordsException as err:
            print(err.response)
        except Exception as err:
            print("Error:", err)
 
def lambda_handler(event=None, context=None):
    """Handler for lambda function"""
    fetch_and_insert_sensor_data()
    

if __name__ == "__main__":
    """
    Add possibility for CLI with `names` specified
    """
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--names", type=str, nargs='*')
    args = parser.parse_args()
    fetch_and_insert_sensor_data(args.names)
