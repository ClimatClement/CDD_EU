#!/bin/bash

#This script loads the models names and corresponding parameters from the file CORDEX_GCM-RCM_couples.yml
#It then constructs a string to be passed to the Python script doing the actual CDS API request (CORDEX_api_request.py)
#It also manages the fact that the CDS would reject too many simultaneous requests by limiting it to 10 simultaneous requests.

source /home/cl-ment-devenet/miniforge3/bin/activate CDD_EU

num_couples=$(yq '.couples | length' CORDEX_GCM-RCM_couples.yml)

{
    for ((i=0; i<num_couples; i++)); do
        name=$(yq ".couples[$i].name" CORDEX_GCM-RCM_couples.yml)
        ensemble_member=$(yq ".couples[$i].ensemble_member" CORDEX_GCM-RCM_couples.yml)
        num_years=$(yq ".couples[$i].start_year | length" "CORDEX_GCM-RCM_couples.yml")

        for ((j=0; j<num_years; j++)); do
            start_year=$(yq ".couples[$i].start_year[$j]" CORDEX_GCM-RCM_couples.yml)
            end_year=$(yq ".couples[$i].end_year[$j]" CORDEX_GCM-RCM_couples.yml)

            if [ "$end_year" -lt 2006 ]; then
                experiment="historical"

                echo "$name|$experiment|$ensemble_member|$start_year|$end_year"

            else
                for k in "rcp_4_5" "rcp_8_5" ; do
                    experiment=$k
                    IFS='|' read -ra parts <<< "$name"
                    gcm="${parts[0]#\"}"
                    rcm="${parts[1]%\"}"
                    ensemble_member=$(yq ".couples[$i].ensemble_member" CORDEX_GCM-RCM_couples.yml)
                    if [[ "ichec_ec_earth" == "$gcm" ]] && [[ "dmi_hirham5" == "$rcm" ]] && [[ "$k" = "rcp_4_5" ]]; then
                            ensemble_member="r3i1p1"
                    fi
                    echo "$name|$experiment|$ensemble_member|$start_year|$end_year"
                done
            fi
        done
    done
} | xargs -P 10 -I {} python3 CORDEX_api_request.py {}