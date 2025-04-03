import xarray as xr
import matplotlib.pyplot as plt
import os
from config import OUTPUT_DATA_PATH

data=xr.open_dataset(os.path.join(OUTPUT_DATA_PATH,'CERRA_test.nc'))
data_2=xr.open_dataset(os.path.join(OUTPUT_DATA_PATH,'CERRA_test_1W.nc'))

lati=int(len(data.y)/2)
loni=int(len(data.x)/2)

data.tp.isel(y=lati,x=loni,valid_time=slice(0,7)).plot()
plt.show()

data_2.tp.isel(y=lati,x=loni,valid_time=slice(0,50)).plot()
plt.show()