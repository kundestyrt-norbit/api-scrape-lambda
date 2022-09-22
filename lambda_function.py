from cgi import print_arguments
import os
from traceback import print_tb
from urllib import response
import requests
from datetime import timedelta, datetime, timezone
from pprint import pprint
import json
import boto3

TIMESTREAM_DATATYPES = ("DOUBLE", "BIGINT", "VARCHAR", "BOOLEAN")

SECRET = os.environ["SECRET"]
ACCESS_KEY = os.environ["ACCESS_KEY"]
DATABASE_NAME = os.environ["DATABASE_NAME"]
TABLE_NAME = os.environ["TABLE_NAME"]
COMPANY_ID = os.environ["COMPANY_ID"]

assert SECRET
assert ACCESS_KEY
assert DATABASE_NAME
assert TABLE_NAME
AUTH_HEADERS = {
    "X-API-SECRET": SECRET,
    "X-API-KEY": ACCESS_KEY
} 

def get_measurement(data, schema):
    measurements = []
    for key, value in schema.items():
        if isinstance(value, str):
            measurements.append((key, data[key], value))
        elif isinstance(value, dict):
            measurements += get_measurement(data[key],value)
        else:
            raise ValueError
    return measurements

def extract_device_data(device_data, sensor_schema):
    records = []
    for data in device_data:
        measure_values = []
        measurements = get_measurement(data, sensor_schema)
        for schema in measurements:
            measure_values.append({'Name': schema[0], 'Value': str(schema[1]), 'Type': schema[2]})
        record = {
            'Dimensions': [
                {'Name': 'gateway_id', 'Value': str(data['gatewayId'])},
                {'Name': 'tagId', 'Value': str(data['deviceId'])},
            ],
            'Time': str(int(round(datetime.strptime(data['timestamp'], '%Y-%m-%dT%H:%M:%S.%f').timestamp()*1000))),
            'MeasureName': 'sensor_measurements',
            'MeasureValueType': 'MULTI',
            'MeasureValues': measure_values
        }
        if len(measure_values) != 0:
            records.append(record)
    return records

def get_device_data(device_id, start_time, end_time):
    res = requests.get(f"http://api.norbitiot.com/api/td/device/{COMPANY_ID}/{device_id}/period/{start_time}/{end_time}", headers=AUTH_HEADERS).json()
    return res

def lambda_handler(event=None, context=None):
    gmt_datetime = datetime.now(timezone.utc) - timedelta(minutes=40)
    start_time = gmt_datetime.strftime("%Y-%m-%dT%H:%M")
    end_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
    sensor_data_schema = open("sensor_data.json")
    schema = json.load(sensor_data_schema)
    records = []
    for sensor in schema:
        records += extract_device_data(get_device_data(sensor["id"], start_time, end_time), sensor["data"])

    client = boto3.client("timestream-write")

    try:
        result = client.write_records(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME,
                                            Records=records, CommonAttributes={})
        print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
    except client.exceptions.RejectedRecordsException as err:
        print(err)
    except Exception as err:
        print("Error:", err)

if __name__ == "__main__":
    lambda_handler()
