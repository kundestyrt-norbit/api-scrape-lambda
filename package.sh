#!/bin/bash
package_name=deployment-package
pipenv run pip freeze > requirements.txt
pip install --target ./package requests
cd package
zip -r ../$package_name.zip .
cd ..
zip -g $package_name.zip lambda_function.py