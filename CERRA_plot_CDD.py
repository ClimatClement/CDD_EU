import os
import re
import xarray as xr
import pandas as pd
import numpy as np
import datetime as dt
from pathlib import Path

from config import OUTPUT_DATA_PATH, CERRA_approx_resolution_degrees, TARGETS
from functions import Plot_EUR_map, Plot_local_time_series

#CONSTRUCTION

sourcedir=os.path.join(OUTPUT_DATA_PATH,'CDD','CERRA')
all_fpaths=[fp for fp in Path(sourcedir).glob('*.nc')]
all_fpaths.sort()

years = [pd.to_datetime(re.search(r'\d{4}', f).group(), format='%Y') for f in list(map(str,all_fpaths))]

datasets = [xr.open_dataset(f).expand_dims(time=[t]) for f, t in zip(all_fpaths, years)]

ds = xr.concat(datasets, dim='time')

Plot_EUR_map(ds.max(dim='time').CDD,'Longest period of Consecutive Dry Days observed from 1984 to 2022','Number of days',pinpoints=TARGETS)

#Selection of the nearest data point to the target
#Very weird way of doing it IMO
for target in TARGETS:
    target_coords=TARGETS[target]
    north_bound=target_coords['lat']+CERRA_approx_resolution_degrees/2
    south_bound=target_coords['lat']-CERRA_approx_resolution_degrees/2
    west_bound =target_coords['lon']-CERRA_approx_resolution_degrees/2
    east_bound =target_coords['lon']+CERRA_approx_resolution_degrees/2

    target_data = ds.where((ds.latitude<north_bound)&(ds.latitude>south_bound)&(ds.longitude>west_bound)&(ds.longitude<east_bound),drop=True)
    counter=0
    while ((target_data.x.shape[0]!=1)|(target_data.y.shape[0]!=1))&(counter<10):
        #Moving the bounds until we capture a single data point.
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
    ref_period_year0=1984
    ref_period_year1=2013
    med_val=np.median(target_data.sel(time=slice(dt.date(ref_period_year0,1,1),dt.date(ref_period_year1,12,31)))['CDD'].values.flatten())
    med_val_label=f'Median of the period {ref_period_year0} - {ref_period_year1}'
    Plot_local_time_series(target_data['time'].dt.year.values,target_data['CDD'].values.flatten(),med_val,med_val_label,title,color='#907700')