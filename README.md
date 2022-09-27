# api-scrape-lambda
Lambda code for scraping the sensor api

Can also be used as a standalone script by exporting correct env variables and running:

`aws-vault exec <vault-profile> --region=eu-west-1 python lambda_function.py -- -d <num-days> -ids [sensor-ids...]`

Exporting vars can be done by:
`export $(cat <env file> | sed '/^#/d' | xargs)`