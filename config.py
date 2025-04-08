from pathlib import Path

#LOCAL PATHS
BDD_PATH = Path('/media/cl-ment-devenet/Partage/bdd')

OUTPUT_DATA_PATH = Path('/media/cl-ment-devenet/Partage/bdd/Produits')

#PARAMETERS
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