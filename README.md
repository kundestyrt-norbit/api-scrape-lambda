# api-scrape-lambda
Lambda code for scraping the sensor api

Can also be used as a standalone script by exporting correct env variables and running:

`aws-vault exec <vault-profile> --region=eu-west-1 python lambda_function.py -- -d <num-days> -ids [sensor-ids...]`

Exporting vars can be done by:
`export $(cat <env file> | sed '/^#/d' | xargs)`

The schema defined in [sensor_data.json](./sensor_data.json) describes the dataformat which is expected from the sensor data endpoint. Each element in the file should contain the sensor id and recursively define the fields where the data is located.

I.E:

```
[{id":1, "data":{
    "sensor_data_1": "BIGINT",
    "sensor_data_2":{
        "sensor_data_2_part_1":"DOUBLE",
        "sensor_data_2_part_2": "VARCHAR",
        "more_custom_data": {...}
        }
},{...}]
```