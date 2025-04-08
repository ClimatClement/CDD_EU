import cdsapi
import os
import sys
from config import BDD_PATH

#This script assembles and sends the CDS API request.
#It also creates the corresponding directories to save the data and ignores requests if the result file already exists.

variable="mean_precipitation_flux"

GCM,RCM,experiment,ens_member,start_year,end_year=sys.argv[1].split('|')

writedir=os.path.join(BDD_PATH,'CORDEX',GCM,RCM,experiment,ens_member,variable)

os.makedirs(writedir,exist_ok=True)

os.chdir(writedir)

target=f'CORDEX_EUR11_{GCM}_{RCM}_{experiment}_{ens_member}_{start_year}_{end_year}_{variable}.nc'

if os.path.exists(os.path.join(writedir,target)):
    print(f'Ignoring file {target}. File already exists')
    exit()

print(f'Requesting file {target}')

dataset = "projections-cordex-domains-single-levels"
request = {
    "domain": "europe",
    "experiment": experiment,
    "horizontal_resolution": "0_11_degree_x_0_11_degree",
    "temporal_resolution": "daily_mean",
    "variable": [variable],
    "gcm_model": GCM,
    "rcm_model": RCM,
    "ensemble_member": ens_member,
    "start_year": [start_year],
    "end_year": [end_year],
}

client = cdsapi.Client()
client.retrieve(dataset, request,target)