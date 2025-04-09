#!/bin/bash

source /home/climatclement/miniforge3/bin/activate CDD_EU

first_years=(1984 1993 2002 2011 2020)
last_years=(1992 2001 2010 2019 2022)
num_years=${#first_years[@]}

num_stations=$(yq '.stations | length' METEOSTAT_weather_stations.yml)

#mkdir /home/climatclement/bdd/METEOSTAT

    for ((i=0; i<num_stations; i++)); do
        name=$(yq ".stations[$i].name" METEOSTAT_weather_stations.yml)
        id=$(yq ".stations[$i].id" METEOSTAT_weather_stations.yml)
        id="${id#\"}"
        id="${id%\"}"
        name="${name#\"}"
        name="${name%\"}"

        for ((j=0; j<num_years; j++)); do
            year0=${first_years[j]}
            year1=${last_years[j]}

            cp METEOSTAT_api_request.txt "temp_${id}${year0}${year1}.txt"

            sed -i -e "s/ID/${id}/g" "temp_${id}${year0}${year1}.txt"
            sed -i -e "s/YEAR0/${year0}/g" "temp_${id}${year0}${year1}.txt"
            sed -i -e "s/YEAR1/${year1}/g" "temp_${id}${year0}${year1}.txt"
            
            curl -K "temp_${id}${year0}${year1}.txt" -o "/home/climatclement/bdd/METEOSTAT/${name}_station_${year0}_${year1}.json"

            sleep 1
        done
    done

rm temp_*