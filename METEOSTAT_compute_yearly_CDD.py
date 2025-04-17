import os
import yaml
import json
import random
import pandas as pd
import numpy as np
from pathlib import Path

from config import BDD_PATH, OUTPUT_DATA_PATH

#THIS SCRIPT IS ABANDONNED. THE REASONS ARE :
# - The data source METEOSTAT is deemed unreliable
# - The precipitation series contain to mainy missing values
#       - 170 to 350 missing days per year for KAUNAS station
#       - over 200 missing days per year from 1984 to 2006 for IASI and the suspicion of data being filled with "0" afterward
#       - over 150 missing days per year from 1984 to 2004 for MONT-DE-MARSAN.
#Consequently, one cannot extract usefull climatic information from these series.
#This script at least contains a usefull function `A_random_year()` that creates a random year of values resampling existing values.

def A_random_year(df,window:int=5):
    '''
    This function creates a random year of values resampling existing values.

    INPUT
    df  pandas.DataFrame with one column of data and, column 'dayofyear' and more than a years worth of data
    window  int the number of day before and after the current day to sample.

    OUTPUT
    random_year list    a list of length 1 year (365 of 366 days depending on the input data) containing values sampled from the input data for each year day ± the window.
                        e.g. with a window of 5 days the first value (Jan 1st), is a value randomely picked among all existing values between Dec 26th and Jan 5th all years considered.
    '''

    daily_sample=[pd.DataFrame(ele[1]).dropna().drop(columns=['dayofyear']) for ele in df.groupby('dayofyear')]

    random_year=[]

    for day in range(len(daily_sample)):
        doy_range = [(day + i - 1) % len(daily_sample) + 1 for i in range(-window, window + 1)]
        random_year.append(random.choice(np.concatenate([daily_sample[window_day-1].values for window_day in doy_range]))[0])

    return(random_year)

#Paths
sourcedir=os.path.join(BDD_PATH,'METEOSTAT')

workdir=os.path.join(OUTPUT_DATA_PATH,'CDD','METEOSTAT')
os.makedirs(workdir,exist_ok=True)

all_fpaths=[fp for fp in Path(sourcedir).glob('*.json')]

#Read data
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

df_precip['dayofyear']=df_precip['date'].dt.dayofyear

#Filling missing values
random_year_df=pd.DataFrame({'dayofyear':np.arange(1,367)})
initialisation=True
for year in df_precip['year'].unique():
    for station in stations_dict:
        random_year_df[station]=A_random_year(df_precip[[station,'dayofyear']])
    if initialisation:
        filling_df=random_year_df.copy()
        initialisation=False
        continue
    filling_df=pd.concat([filling_df,random_year_df])

df_precip_filled=pd.DataFrame()
for station in stations_dict:
    df_precip_filled[station]=df_precip[station].fillna(filling_df.reset_index()[station])

#Mapping dry days
#The raw precip data
df_drydays_raw=df_precip.drop(columns=['date','dayofyear','year']).map(lambda x: np.nan if pd.isna(x) else (1 if x < 2 else 0))
df_drydays_raw['year']=df_precip['year'].values

#The filled data (where NaN values have been replaced with statisticaly realistic values)
df_drydays=df_precip_filled.map(lambda x: np.nan if pd.isna(x) else (1 if x < 2 else 0))
df_drydays['year']=df_precip['year'].values

#Count the number of NaN values per year.
number_of_NaN_values_per_year=df_precip.isnull().groupby(df_precip['year']).sum().drop(columns=['date','year','dayofyear'])

#Decision to stop this developpement.