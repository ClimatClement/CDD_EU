#!/bin/bash

source /home/climatclement/miniforge3/bin/activate CDD_EU

for year in `cat CERRA_years.txt`
do
python3 CERRA_api_request.py $year &
done