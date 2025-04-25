import os
import yaml
import traceback
import shutil
import xarray as xr
import numpy as np
from datetime import datetime
from pathlib import Path
from joblib import Parallel, delayed

from config import BDD_PATH, OUTPUT_DATA_PATH,TRASH_PATH
from functions import Groupby_consecutive_ones, Write_to_log

#VARIABLES DECLARATION
dry_days_threshold = 2 #kg.m-2.day-1
variable = 'pr'

#PATHS DECLARATION AND CREATION OF THE MODELS AND SCENARIOS LIST
yaml_file_path = "CORDEX_GCM-RCM_couples.yml"
sourcedir=os.path.join(BDD_PATH, 'CORDEX')
workdir = os.path.join(OUTPUT_DATA_PATH, 'CDD/CORDEX')

with open(yaml_file_path, 'r') as yaml_file:
    yaml_content = yaml.safe_load(yaml_file)
    model_names = [ele['name'] for ele in yaml_content['couples']]

all_fpaths = {}
for model_name in model_names:
    GCM_name,RCM_name = model_name.split("|")
    model_path =os.path.join(sourcedir,GCM_name,RCM_name)
    all_fpaths[model_name] = [f for f in Path(model_path).rglob("*.nc")]

models_and_scenarios_list = [(model_couple, scenario) for model_couple in all_fpaths.keys() for scenario in ['historical', 'rcp_4_5', 'rcp_8_5']]

#MAIN FUNCTION
def Process_model_couple(models_and_scenario):
    '''
    This function computes the maximum number of consecutive dry days (CDD) for a given couple of GCM and RCM models and a scenario.
    '''
    os.makedirs(os.path.join(workdir,f"logs"), exist_ok=True)
    log_file = os.path.join(workdir,f"logs/simulation_{'-'.join(models_and_scenario)}.log")

    start=datetime.now()
    Write_to_log(log_file,f'Job started at {start}',overwrite=True)

    try:
        model_couple, scenario = models_and_scenario
        GCM_name, RCM_name = model_couple.split("|")

        Write_to_log(log_file,f'Processing model couple: {model_couple} - {scenario}')
        print(f'Processing model couple: {model_couple} - {scenario}')

        writedir = os.path.join(workdir, GCM_name, RCM_name, scenario)
        os.makedirs(writedir, exist_ok=True)

        scenario_model_couple_paths = [p for p in all_fpaths[model_couple] if scenario in str(p)]
        fname = os.path.basename(scenario_model_couple_paths[0])
        out_fname = '_'.join(fname.split('_')[:-1] + ['CDD.nc'])
        out_fpath = os.path.join(writedir, out_fname)

        if os.path.exists(out_fpath):
            Write_to_log(log_file,f'File {out_fpath} already exists. Skipping…')
            print(f'Already exists. Skipping : {model_couple} - {scenario}')
            end=datetime.now()
            Write_to_log(log_file,f'Job canceled at {end}')
            Write_to_log(log_file,f'Time taken : {end-start}')
            return

        temp_dir=os.path.join(writedir,f"temp_{GCM_name}_{RCM_name}_{scenario}")
        os.makedirs(temp_dir, exist_ok=True)
        for fpath in sorted(scenario_model_couple_paths):   #Input data files come either as single year files or as multi-year files, so there is a bunch of input files for each model couple and scenario.
            Write_to_log(log_file,f'Input file : {fpath}')

            data = xr.open_dataset(fpath, engine='netcdf4')

            time_dim = [n for n in data.variables if ('time' in n)&('bnds' not in n)][0]    #Time dimension can either be "valid_time", "time_counter", "time", etc.

            pole_rotation_var_name = [n for n in data.variables if ('rotated' in n)|('Lambert' in n)][0]

            #Get the coordinates of the dataset without the time dimension to apply to the output dataset.
            coords_no_time = data.drop_vars(time_dim).coords

            #Compute the dry days array. Input data is provided in kg.m-2.s-1, so we multiply by 86400 to convert to kg.m-2.day-1 to match the threshold unit.
            dry_days = xr.where(86400 * data.pr <= dry_days_threshold, 1, 0)
            
            years = np.unique(dry_days[time_dim].dt.year)

            for year in years:

                Write_to_log(log_file,f"working on year {year}")

                out_year_fpath=os.path.join(temp_dir,f"CDD_{year}.nc")

                if os.path.exists(out_year_fpath):
                    Write_to_log(log_file,f"Year {year} already exists. Skipping…")
                    continue

                y_dry_days = dry_days.sel({time_dim: slice(f'{year}-01-01', f'{year + 1}-01-01')})

                nb_days = len(y_dry_days[time_dim])

                cumsum_result = xr.apply_ufunc(
                    Groupby_consecutive_ones,
                    y_dry_days,
                    input_core_dims=[[time_dim]],
                    output_core_dims=[[time_dim]],
                    vectorize=True,
                    dask='parallelized',
                    output_dtypes=[dry_days.dtype]
                )

                yCDD_array = cumsum_result.max(dim=time_dim)
                yCDD_array.name = 'CDD'

                temp_CDD = xr.Dataset(
                    data_vars=dict(
                        CDD=(['year']+[i for i in list(data[variable].dims) if time_dim not in i], np.expand_dims(yCDD_array, axis=0)),
                        nb_days=(["year"], [nb_days]),
                        rotated_pole=data[pole_rotation_var_name],
                    ),
                    coords={
                        'year':[year],
                        **coords_no_time,
                        },
                    attrs=dict(description="Maximum Consecutive Dry Days (CDD) and number of days per year."),
                )

                temp_CDD.to_netcdf(out_year_fpath)

        final_CDD=xr.open_mfdataset(os.path.join(temp_dir,f"CDD_*.nc"), combine='by_coords')

        final_CDD.to_netcdf(out_fpath)

        shutil.move(temp_dir, TRASH_PATH)

        Write_to_log(log_file,f'File saved as : {out_fpath}')
        end=datetime.now()
        Write_to_log(log_file,f'Job completed successfully at {end}')
        Write_to_log(log_file,f'Time taken : {end-start}')
        print(f'Job completed successfully : {model_couple} - {scenario}')

    except Exception as e:
        Write_to_log(log_file,f"Error in simulation {models_and_scenario}:\n")
        Write_to_log(log_file,traceback.format_exc())
        end=datetime.now()
        Write_to_log(log_file,f'Job failed at {end}')
        Write_to_log(log_file,f'Time taken : {end-start}')
        print(f'Job FAILED : {model_couple} - {scenario}\nLog file: {log_file}')
        return None

Parallel(n_jobs=3)(delayed(Process_model_couple)(models_and_scenario) for models_and_scenario in models_and_scenarios_list)