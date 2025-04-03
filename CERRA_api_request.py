import cdsapi
import os
import sys
from config import OUTPUT_DATA_PATH

year=sys.argv[1]
writedir=OUTPUT_DATA_PATH
#os.chdir(writedir)
target=f'CERRA_{year}_tp.nc'

dataset = "reanalysis-cerra-single-levels"
request = {
    "variable": ["total_precipitation"],
    "level_type": "surface_or_atmosphere",
    "data_type": ["reanalysis"],
    "product_type": "forecast",
    "year": [year],
    "month": [
        "01", "02", "03",
        "04", "05", "06",
        "07", "08", "09",
        "10", "11", "12"
    ],
    "day": [
        "01", "02", "03",
        "04", "05", "06",
        "07", "08", "09",
        "10", "11", "12",
        "13", "14", "15",
        "16", "17", "18",
        "19", "20", "21",
        "22", "23", "24",
        "25", "26", "27",
        "28", "29", "30",
        "31"
    ],
    "time": ["00:00"],
    "leadtime_hour": ["24"],    #In CERRA, tp is cumulative by setting time 00:00 and leadtime_hour 24 we get the total amount of precipitation of the previous 24 hours. In consequence, the time stamp 2-1-1984 refers to the precipitations of the 1-1-1984.
    "data_format": "netcdf"
}
print(year,' : ',target)
print()
print(request)
print()
#client = cdsapi.Client()
#client.retrieve(dataset, request, target).download()
#os.chdir('/home/climatclement/Documents/CODE/CDD_EU')
