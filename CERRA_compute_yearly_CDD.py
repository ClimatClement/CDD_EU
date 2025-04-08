import os
import xarray as xr
from pathlib import Path

from config import BDD_PATH, OUTPUT_DATA_PATH
from functions import Groupby_consecutive_ones

#From each CERRA annual data file, this scripts computes the longest period of Consecutive Dry Days and saves the result as a new NetCDF file.

dry_days_threshold = 2 #kg.m-2.day-1

sourcedir=os.path.join(BDD_PATH,'CERRA')

workdir=os.path.join(OUTPUT_DATA_PATH,'CDD','CERRA')
os.makedirs(workdir,exist_ok=True)

all_fpaths=[fp for fp in Path(sourcedir).glob('*.nc')]

for source_fpath in all_fpaths:
    fname=os.path.basename(source_fpath)
    out_fname='_'.join(fname.split('_')[:-1]+['CDD.nc'])
    out_fpath=os.path.join(workdir,out_fname)
    
    if os.path.exists(out_fpath):
        print(f'File {out_fpath} already exists. Skipping…')
        continue

    print(f'Input file : {source_fpath}')

    data=xr.open_dataset(source_fpath)

    dry_days=xr.where(data.tp <= dry_days_threshold, 1, 0)

    cumsum_result = xr.apply_ufunc(
        Groupby_consecutive_ones,
        dry_days,
        input_core_dims=[['valid_time']],
        output_core_dims=[['valid_time']],
        vectorize=True,
        dask='parallelized',
        output_dtypes=[dry_days.dtype]
    )

    CDD=cumsum_result.max(dim='valid_time')
    CDD.name='CDD'

    CDD.to_netcdf(out_fpath)
    print(f'File saved as : {out_fpath}')