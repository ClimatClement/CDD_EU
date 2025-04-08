#A Python file with chunks of code in construction
#It is a test and a playground area
#This is never meant to be executed as it is

import os
import xarray as xr
import numpy as np
from itertools import groupby
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import cartopy.crs as ccrs
import cartopy.feature as cfeature

from config import BDD_PATH, OUTPUT_DATA_PATH

def groupby_consecutive_ones(arr):
    result = np.zeros_like(arr)
    idx = 0
    for key, group in groupby(arr):
        group_list = list(group)
        if key == 1:
            for i in range(len(group_list)):
                result[idx + i] = i + 1
        idx += len(group_list)
    return result

year=2003
if 'data' not in [k for k in locals()]:
    data=xr.open_dataset(os.path.join(BDD_PATH,f'CERRA/CERRA_{year}_tp.nc'))
    data_2=xr.open_dataset(os.path.join(BDD_PATH,f'CERRA/CERRA_{year+1}_tp.nc'))

lati=int(len(data.y)/2)
loni=int(len(data.x)/2)

dry_days=xr.where(data.tp<=2,1,0)

cumsum_result = xr.apply_ufunc(
    groupby_consecutive_ones,
    dry_days, #['tp'],
    input_core_dims=[['valid_time']],
    output_core_dims=[['valid_time']],
    vectorize=True,
    dask='parallelized',
    output_dtypes=[dry_days.dtype] #['tp'].dtype]
)
if 'CDD' not in [k for k in locals()]:
    CDD=cumsum_result.max(dim='valid_time')

fig = plt.figure(figsize=(25, 12))
ax = plt.axes(projection=ccrs.PlateCarree())
ax.set_title("Longest period of CDD", fontsize=14, fontweight='bold')

# Add map features
ax.coastlines()
ax.add_feature(cfeature.BORDERS, linewidth=0.5)
ax.add_feature(cfeature.RIVERS, linewidth=0.5)
ax.add_feature(cfeature.LAND, facecolor='lightgray')
ax.add_feature(cfeature.OCEAN, facecolor='lightblue')

# Plot the data
mesh = ax.pcolormesh(
    CDD.longitude,
    CDD.latitude,
    CDD,
    transform=ccrs.PlateCarree(),
    cmap='Reds',
    vmin=0,
    vmax=180
)

# Add colorbar
cbar = plt.colorbar(mesh, ax=ax, orientation='vertical', shrink=0.6, pad=0.05)
cbar.set_label("Longest period of CDD", fontsize=12)

# Set extent if needed (auto-zoom to data)
ax.set_extent([
    -25,
    41,
    35,
    72
], crs=ccrs.PlateCarree())

gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
gl.top_labels = False
gl.right_labels = False
gl.xlocator = mticker.FixedLocator(np.arange(-180, 181, 5))
gl.ylocator = mticker.FixedLocator(np.arange(-90, 91, 5))

plt.show()

#data.tp.isel(y=lati,x=loni,valid_time=slice(0,7)).plot()
#plt.show()
#
#data_2.tp.isel(y=lati,x=loni,valid_time=slice(0,50)).plot()
#plt.show()

###FUTUR DEV : Masker CERRA par NUTS
import re
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point
from pyproj import CRS
from config import BDD_PATH

sourcedir=os.path.join(OUTPUT_DATA_PATH,'CDD','CERRA')
all_fpaths=[fp for fp in Path(sourcedir).glob('*.nc')]
all_fpaths.sort()

years = [pd.to_datetime(re.search(r'\d{4}', f).group(), format='%Y') for f in list(map(str,all_fpaths))]

datasets = [xr.open_dataset(f).expand_dims(time=[t]) for f, t in zip(all_fpaths, years)]

ds = xr.concat(datasets, dim='time')

limit_NUTS=gpd.read_file(os.path.join(BDD_PATH,'EUROSTAT/NUTS_RG_20M_2024_3035.geojson'))

CERRA_lambert_crs = CRS.from_proj4(
    "+proj=lcc +lat_1=50 +lat_2=50 +lat_0=50 +lon_0=8 "
    "+x_0=0 +y_0=0 +R=6371229 +units=m +no_defs"
)

limit_NUTS_lambert = limit_NUTS.to_crs(CERRA_lambert_crs)

xx, yy = np.meshgrid(ds['longitude'], ds['latitude'])
points = gpd.GeoSeries([Point(x, y) for x, y in zip(xx.ravel(), yy.ravel())], crs=CERRA_lambert_crs)

# Mask
mask = points.within(limit_NUTS_lambert[limit_NUTS['NUTS_NAME']=='Eure-et-Loir'].geometry).values.reshape(xx.shape)

# Apply mask
masked_ds = ds.where(mask)