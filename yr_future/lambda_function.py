import os
import requests
from datetime import datetime
import json
import boto3
import math
from dotenv import load_dotenv

load_dotenv()

TIMESTREAM_DATATYPES = ("DOUBLE", "BIGINT", "VARCHAR", "BOOLEAN")

DATABASE_NAME = os.getenv("DATABASE_NAME")
TABLE_NAME = os.getenv("TABLE_NAME")

assert DATABASE_NAME
assert TABLE_NAME

def get_measurement(data: pd.Dataframe, time_delta: int):
    """Extract measurements from json object

    :param data: json-object to extract data from
    :param time_delta: prediction for time_delta hours in the future
    :return: measurements to be inserted into timestream
    """
    measurements = []
    for name in ["air_temperature", "relative_humidity", "wind_speed"]:
        measurements.append({
            "Name": f"{time_delta}h_{name}",
            "Value": str(data["data"]["instant"]["details"][name]),
            "Type": "DOUBLE"
        })
    
    # wind direction is set together of sin and cos to avoid 359 -> 0 
    wind_direction = float(data["data"]["instant"]["details"]["wind_from_direction"])
    cos_val = math.cos(math.radians(wind_direction))
    sin_val = math.sin(math.radians(wind_direction))
    measurements.append({
        "Name": f"{time_delta}h_wind_direction_cos",
        "Value": str(cos_val),
        "Type": "DOUBLE"
    })
    measurements.append({
        "Name": f"{time_delta}h_wind_direction_sin",
        "Value": str(sin_val),
        "Type": "DOUBLE"
    })
    measurements.append({
        "Name": f"{time_delta}h_percipitation",
        "Value": str(data["data"]["next_1_hours"]["details"]["precipitation_amount"]),
        "Type": "DOUBLE"
    })
    return measurements

def get_and_extract_yr_data(lat: float, lon: float, gateway_id: int):
    """Gets the predicted data from yr.no by using the API to yr.no,
    and returns the rows to be inserted to the timestream database

    :param lat: Lat of the gateway, to be stored in the database. Used for finding weather forecast
    :param lon: Lon of the gateway, to be stored in the database. Used for finding weather forecast
    :param gateway_id: Gateway id to be stored in the database
    :return: rows to be inserted into database
    """
    # Must be included to not get 403 error
    header = {
        'User-Agent': 'NorbitMose github.com/kundestyrt-norbit',
    }
    # Yr.no API states that lat lon only should have 4 decimal precision
    yr_data = requests.get(f"https://api.met.no/weatherapi/locationforecast/2.0/compact.json?lat={round(lat, 4)}&lon={round(lon, 4)}", headers=header).json()
    records = []
    measure_values = []
    # get prediction for next 24 hours
    for i, data in enumerate(yr_data["properties"]["timeseries"]):
        if i < 24:
            measure_values.extend(get_measurement(data=data,time_delta=i+1))
    record = {
        'Dimensions': [
            {'Name': 'lat', 'Value': str(lat)},
            {'Name': 'lon', 'Value': str(lon)},
            {'Name': 'gateway_id', 'Value': str(gateway_id)},

        ],
        'Time': str(int(round(datetime.now().replace(second=0, microsecond=0, minute=0).timestamp()*1000))),
        'MeasureName': 'yr_prediction',
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
            records += get_and_extract_yr_data(lat=point['lat'], lon=point['lon'], gateway_id=point['gatewayId'])
    if len(records) == 0:
        raise ValueError("The specified ids has no matching schemas.")

    client = boto3.client("timestream-write")
    for window_i in range(0, len(records), 100):
        record_window = records[window_i: window_i + 100]
        try:
            result = client.write_records(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME,
                                                Records=record_window, CommonAttributes={'Version': round(datetime.now().timestamp())})
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
