import pickle
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from whittaker_eilers import WhittakerSmoother
import matplotlib.pyplot as plt
from tqdm import tqdm
from glob import glob
import os, sys
import concurrent.futures

def generate_date_pairs(year):
    start_date = datetime(year, 1, 1)
    date_pairs = []
    while start_date.year == year:
        end_date = start_date + timedelta(days=11)
        if end_date.year != year:
            break
        date_pairs.append([start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')])
        start_date = end_date + timedelta(days=1)
    return date_pairs

def prepare_dates():
    list_date = []
    for year in [2021, 2022, 2023]:
        list_date.extend(generate_date_pairs(year))
    list_date = [f"{start.replace('-', '')}_{end.replace('-', '')}" for start, end in list_date]
    return list_date

def do_preparation(ls_pickle):
    with open(ls_pickle, 'rb') as file:
        dt_pkl = pickle.load(file)
    return dt_pkl

def do_imputation(idpoint, dt_pkl, list_date):
    u = dt_pkl.query('idpoint == @idpoint').sort_values('periode')
    temp = pd.DataFrame()
    for j in list_date:
        item = u.query('periode == @j')
        if item.empty:
            item = pd.DataFrame({
                'idpoint': [idpoint],
                'MGRS': [u.MGRS.unique()[0]],
                'Sigma0_VH_db': [0],
                'Sigma0_VV_db': [0],
                'periode': [j]
            })
        temp = pd.concat([temp, item], ignore_index=True)
    
    temp['weight'] = temp['Sigma0_VH_db'].apply(lambda y: 0 if y == 0 else 1)
    whittaker_smoother = WhittakerSmoother(lmbda=1, order=2, data_length=temp.shape[0], weights=temp['weight'])
    temp['Sigma0_VH_db_interp'] = whittaker_smoother.smooth(temp['Sigma0_VH_db'])
    temp['Sigma0_VV_db_interp'] = whittaker_smoother.smooth(temp['Sigma0_VV_db'])
    temp['Sigma0_VH_db_imputted'] = temp.apply(lambda y: y['Sigma0_VH_db'] if y['Sigma0_VH_db'] != 0 else y['Sigma0_VH_db_interp'], axis=1)
    temp['Sigma0_VV_db_imputted'] = temp.apply(lambda y: y['Sigma0_VV_db'] if y['Sigma0_VV_db'] != 0 else y['Sigma0_VV_db_interp'], axis=1)
    
    with open(f'/data/ksa/04_Data_Preprocessing/temp_imputation/{idpoint}.pkl', 'wb') as file:
        pickle.dump(temp, file)

def main(kdprov):
    print('=================================================')
    print('Begin Imputation for Province: ', kdprov)
    pickle_prov = glob(f'/data/ksa/03_Sampling/data/{kdprov}/*.pkl')
    print('Found:', len(pickle_prov), 'data')

    list_date = prepare_dates()

    for pickle_file in pickle_prov:
        mgrs = os.path.basename(pickle_file).replace('.pkl', '').replace('sampling_', '')
        output=f'/data/ksa/04_Data_Preprocessing/{kdprov}/{mgrs}_imputed_data.pkl'
        if not os.path.exists(output):
            print('-------------------------------------------------------')
            print('Start for:', pickle_file)
            dt_pkl = do_preparation(pickle_file)
            list_idpoint = dt_pkl.idpoint.unique()

            num_workers = 30
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = {executor.submit(do_imputation, idpoint, dt_pkl, list_date): idpoint for idpoint in list_idpoint}
                for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
                    future.result()

            print('Finish')
            print('-------------------------------------------------------')
            print('Compiling')

            dt_pkl_list = glob(f'/data/ksa/04_Data_Preprocessing/temp_imputation/*.pkl')
            dt_full = pd.concat([pickle.load(open(file, 'rb')) for file in tqdm(dt_pkl_list)], ignore_index=True)
        
            for file in dt_pkl_list:
                os.remove(file)

            with open(output, 'wb') as file:
                pickle.dump(dt_full, file)

            print('Finish')
            print('-------------------------------------------------------')
        else:
            print(f'The file has been created {ouput} previously. SKIP IT')
    print('Finish')
    print('=================================================')

if __name__ == "__main__":
    kdprov = sys.argv[1]
    main(kdprov)
