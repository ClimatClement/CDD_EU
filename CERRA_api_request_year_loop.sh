#!/bin/bash

#This script gets the list of years from the file CERRA_years.txt
#It then passes each year in turn to the Python script doing the actual CDS API request (CERRA_api_request.py)
#It also manages the fact that the CDS would reject too many simultaneous requests by limiting it to 10 simultaneous requests.

source /home/cl-ment-devenet/miniforge3/bin/activate CDD_EU

cat CERRA_years.txt | xargs -P 10 -I {} python3 CERRA_api_request.py {}