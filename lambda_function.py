import os
import requests
from datetime import timedelta, datetime, timezone
from pprint import pprint
import boto3

COMPANY_ID = 6
DEVICE_IDS = (43)
SECRET = os.environ.get("SECRET")
ACCESS_KEY = os.environ.get("ACCESS_KEY")
DATABASE_NAME = os.environ.get("DATABASE_NAME")
TABLE_NAME = os.environ.get("TABLE_NAME")

assert SECRET
assert ACCESS_KEY
assert DATABASE_NAME
assert TABLE_NAME
AUTH_HEADERS = {
    "X-API-SECRET": SECRET,
    "X-API-KEY": ACCESS_KEY
} 

def get_device_ids():
    r = requests.get(f"http://api.norbitiot.com/api/devices/{COMPANY_ID}/SMART_TAG", headers=AUTH_HEADERS)   
    return [device["id"] for device in r.json()]

def extract_device_data(device_data):
    records = []
    for data in device_data:
        record = {
            'Dimensions': [
                {'Name': 'gateway_id', 'Value': str(data['gatewayId'])},
                {'Name': 'customer_id', 'Value': str(data['customerId'])},
                {'Name': 'tagId', 'Value': str(data['tagId'])},
                {'Name': 'positionLat', 'Value': str(data['positionLat'])},
                {'Name': 'positionLng', 'Value': str(data['positionLng'])}
            ],
            'Time': str(int(round(datetime.strptime(data['timestamp'], '%Y-%m-%dT%H:%M:%S.%f').timestamp()*1000))),
            'MeasureName': 'sensor_measurements',
            'MeasureValueType': 'MULTI',
            'MeasureValues': [
                {'Name': 'temperature', 'Value': str(data['temperature']), 'Type': 'DOUBLE'},
                {'Name': 'humidity', 'Value': str(data['humidity']), 'Type': 'DOUBLE'}
            ]
        }
        records.append(record)
    return records

def get_device_data(device_ids, start_time, end_time):
    responses = []
    for id in device_ids:
        res = requests.get(f"http://api.norbitiot.com/api/td/device/{COMPANY_ID}/{id}/period/{start_time}/{end_time}", headers=AUTH_HEADERS).json()
        responses += res
    return responses

def main():
    gmt_datetime = datetime.now(timezone.utc) - timedelta(minutes=10)
    start_time = gmt_datetime.strftime("%Y-%m-%dT%H:%M")
    end_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
    records = extract_device_data(get_device_data(get_device_ids(), start_time, end_time))
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
    main()