import os
import re
import xarray as xr
import pandas as pd
import numpy as np
from pathlib import Path

from config import OUTPUT_DATA_PATH #, CERRA_approx_resolution_degrees, TARGETS
from functions import Plot_EUR_map, Plot_local_time_series

#In construction

CERRA_approx_resolution_degrees = 0.05
TARGETS = {
    'IASI Romania':{'lat':47.17,'lon':27.63},   #IASI (Romania)
                                                #coordinates: 47.17N, 27.63E, 102.0m
                                                #GHCN-D station code: ROE00108896
                                                #WMO station: 15090
                                                #Found 64 years of data in 1961-2024
    'KAUNAS Lithuania':{'lat':54.88,'lon':23.83},   #KAUNAS (Lithuania)
                                                    #coordinates: 54.88N, 23.83E, 77.0m
                                                    #GHCN-D station code: LH000026629
                                                    #WMO station: 26629
                                                    #Found 90 years of data in 1901-2009
    'MONT-DE-MARSAN France':{'lat':43.91,'lon':0.50},   #MONT-DE-MARSAN (France)
                                                        #coordinates: 43.91N, 0.50E, 59.0m
                                                        #GHCN-D station code: FRE00106203 (get data)
                                                        #WMO station: 7607
                                                        #Found 81 years of data in 1945-2025
}

sourcedir=os.path.join(OUTPUT_DATA_PATH,'CDD','CERRA')
all_fpaths=[fp for fp in Path(sourcedir).glob('*.nc')]
all_fpaths.sort()

years = [pd.to_datetime(re.search(r'\d{4}', f).group(), format='%Y') for f in list(map(str,all_fpaths))]

datasets = [xr.open_dataset(f).expand_dims(time=[t]) for f, t in zip(all_fpaths, years)]

ds = xr.concat(datasets, dim='time')

Plot_EUR_map(ds.max(dim='time').CDD,'CDD moyen 1984-2022','CDD moyen 1984-2022')

for target in TARGETS:
    target_coords=TARGETS[target]
    north_bound=target_coords['lat']+CERRA_approx_resolution_degrees/2
    south_bound=target_coords['lat']-CERRA_approx_resolution_degrees/2
    west_bound =target_coords['lon']-CERRA_approx_resolution_degrees/2
    east_bound =target_coords['lon']+CERRA_approx_resolution_degrees/2

    target_data = ds.where((ds.latitude<north_bound)&(ds.latitude>south_bound)&(ds.longitude>west_bound)&(ds.longitude<east_bound),drop=True)
    counter=0
    while ((target_data.x.shape[0]!=1)|(target_data.y.shape[0]!=1))&(counter<10):
        if target_data.x.shape[0]<1:
            north_bound = north_bound + 0.01 if counter%2==0 else north_bound
            south_bound = south_bound - 0.01 if counter%2==1 else south_bound
        if target_data.x.shape[0]>1:
            north_bound = north_bound - 0.01 if counter%2==0 else north_bound
            south_bound = south_bound + 0.01 if counter%2==1 else south_bound
        if target_data.y.shape[0]<1:
            west_bound = west_bound - 0.01 if counter%2==0 else west_bound
            east_bound = east_bound + 0.01 if counter%2==1 else east_bound
        if target_data.y.shape[0]>1:
            west_bound = west_bound + 0.01 if counter%2==0 else west_bound
            east_bound = east_bound - 0.01 if counter%2==1 else east_bound
        target_data = ds.where((ds.latitude<north_bound)&(ds.latitude>south_bound)&(ds.longitude>west_bound)&(ds.longitude<east_bound),drop=True)
        counter+=1
        
    title=f'CDD at {target} ({target_data.latitude.values[0][0].round(2)}°N, {target_data.longitude.values[0][0].round(2)}°E) from {target_data['time'].dt.year.values.min()} to {target_data['time'].dt.year.values.max()}'
    Plot_local_time_series(target_data['time'].dt.year.values,target_data['CDD'].values.flatten(),title,color='#907700')