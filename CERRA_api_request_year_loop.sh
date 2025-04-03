#!/bin/bash

source /home/cl-ment-devenet/miniforge3/bin/activate CDD_EU

cat CERRA_years.txt | xargs -P 10 -I {} python3 CERRA_api_request.py {}