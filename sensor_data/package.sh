#!/bin/bash
package_name=deployment-package
pipenv install
pipenv run pip freeze > requirements.txt
pip install -r requirements.txt --target ./package
cd package
zip -r ../$package_name.zip .
cd ..
zip -g $package_name.zip lambda_function.py
zip -g $package_name.zip sensor_data.json
