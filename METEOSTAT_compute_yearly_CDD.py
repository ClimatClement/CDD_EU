import os
import yaml
import json
import pandas as pd
import numpy as np
import datetime as dt
from pathlib import Path

from config import BDD_PATH, OUTPUT_DATA_PATH

#CONSTRUCTION

sourcedir=os.path.join(BDD_PATH,'METEOSTAT')

workdir=os.path.join(OUTPUT_DATA_PATH,'CDD','METEOSTAT')
os.makedirs(workdir,exist_ok=True)

all_fpaths=[fp for fp in Path(sourcedir).glob('*.json')]

with open('METEOSTAT_weather_stations.yml') as stream:
    stations_yml=yaml.safe_load(stream)['stations']

stations_dict={}
for ele in stations_yml:
    stations_dict[ele['name']]=ele

data_dict={}

for station in stations_dict:
    station_all_fpaths=[fpath for fpath in all_fpaths if station in str(fpath)]
    station_all_fpaths.sort()
    initialisation=True
    for station_fpath in station_all_fpaths:
        with open(station_fpath, "r") as f:
            data = json.load(f)
            if initialisation:
                df = pd.DataFrame(data["data"])
                df["date"] = pd.to_datetime(df["date"])
                initialisation=False
            else:
                temp_df = pd.DataFrame(data["data"])
                temp_df["date"] = pd.to_datetime(temp_df["date"])
                df=pd.concat([df,temp_df])
        data_dict[station]=df

df_precip=pd.DataFrame({'date':data_dict[station]['date']})
for station in data_dict:
    df_precip[station]=data_dict[station]['prcp'].values

df_precip['year']=df_precip['date'].dt.year.values

df_drydays=df_precip.drop(columns=['date','year']).applymap(lambda x: np.nan if pd.isna(x) else (1 if x < 2 else 0))

df_CDD=pd.DataFrame({'year':np.unique(df_precip['year'].values)})

for year in df_CDD['year']:
    df_CDD.loc(year)=df_precip[df_precip['year']==year].where()