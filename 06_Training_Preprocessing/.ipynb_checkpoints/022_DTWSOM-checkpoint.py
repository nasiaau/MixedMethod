import pickle
import pandas as pd
import polars as pl
import numpy as np
import dtwsom
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import seaborn as sns
import matplotlib.pyplot as plt
from tqdm import tqdm
import sys
def standardize_array(arr,axs):
    # Calculate mean and std along the second axis (axis=1)
    mean = np.mean(arr, axis=axs, keepdims=True)
    std = np.std(arr, axis=axs, keepdims=True)
    standardized_arr = (arr - mean) / std
    return standardized_arr

def som_run(kdprov):
    print('===============================================')
    print('READ THE DATA')
    df_variabce=pd.read_pickle(f'/data/ksa/04_Data_Preprocessing/{kdprov}/02_variance_filtering/variance_filtering.pkl')
    df_training_vh=pd.read_pickle(f'/data/ksa/04_Data_Preprocessing/training-test/{kdprov}/training_imputation_{kdprov}_VH.pkl')
    df_training_vv=pd.read_pickle(f'/data/ksa/04_Data_Preprocessing/training-test/{kdprov}/training_imputation_{kdprov}_VV.pkl')
    df_training_vh=pl.from_pandas(df_training_vh)
    df_training_vv=pl.from_pandas(df_training_vv)
    df_training_vv_var=df_training_vv.filter((pl.col("idsubsegment").is_in(df_variabce.idsubsegment)))
    df_training_vh_var=df_training_vh.filter((pl.col("idsubsegment").is_in(df_variabce.idsubsegment)))
    df_inner = df_training_vv_var.join(df_training_vh_var, on=['idpoint','idsubsegment','idsegment','nth',
                                                           'periode','observation','class','MGRS'], how="inner")
    df_select=df_inner.select(['idpoint','nth','MGRS',
                               'periode','idsubsegment', 'VV_10', 'VV_9', 'VV_8', 'VV_7', 'VV_6', 'VV_5', 'VV_4', 'VV_3', 'VV_2', 'VV_1',
                               'VV_0', 'VH_10', 'VH_9', 'VH_8', 'VH_7', 'VH_6', 'VH_5','VH_4', 'VH_3', 'VH_2', 'VH_1', 'VH_0'])
    bridging_date=pd.read_excel("/data/ksa/03_Sampling/bridging.xlsx", dtype='object', sheet_name="periode_to_date")
    df_variabce['periode_start']=df_variabce['periode'].apply(lambda y: y.split('_')[0][4:])
    df_variabce['periode_end']=df_variabce['periode'].apply(lambda y: y.split('_')[1][4:])
    df_variabce['tahun']=df_variabce['periode'].apply(lambda y: y[:4])
    df_variabce=df_variabce.merge(bridging_date.query('is_kabisat==0')[['periode_start','periode_end','id_per_image']])
    df_variabce['periode_y']=df_variabce.apply(lambda y: str(y.tahun)+'_'+str(y.id_per_image).zfill(2),axis=1)
    df_variabce['periode_x']=df_variabce['periode']
    df_variabce['periode']=df_variabce['periode_y']
    print('FILTERING USING VARIANCE FILTERING')
    df_10_variance=df_variabce.query('less_q10==True')
    print('PREPARING DATA FOR DTW')
    data_for_dtw=df_select.join(pl.from_dataframe(df_10_variance[['periode','idsubsegment','obs']]),
               on=['idsubsegment','periode'],how='inner').to_pandas()
    data_for_dtw_drop=data_for_dtw.drop(columns=['MGRS'],axis=1)
    vv_median=data_for_dtw_drop.groupby(['idsubsegment','periode','obs'])[[f'VV_{i}' for i in range(0,11)]].agg('median').reset_index()
    vh_median=data_for_dtw_drop.groupby(['idsubsegment','periode','obs'])[[f'VH_{i}' for i in range(0,11)]].agg('median').reset_index()
    gab_median=vv_median.merge(vh_median)
    gab_median.obs.value_counts()
    gab_median_frac=gab_median.groupby('obs').sample(frac=0.3,random_state=1234)
    print('CREATING ARRAY FOR DTW')
    np_dtw=np.zeros((gab_median_frac.shape[0],2,11))
    np_dtw[:,0,:]=gab_median_frac.iloc[:,:][[f'VV_{i}' for i in range(0,11)]].to_numpy()
    np_dtw[:,1,:]=gab_median_frac.iloc[:,:][[f'VH_{i}' for i in range(0,11)]].to_numpy()
    print('NORMALIZE')
    np_dtw_std=standardize_array(np_dtw,2)
    print('RUN SOM WITH DTW DISTANCE BASED')
    som=dtwsom.MultiDtwSom(20,20, np_dtw.shape[2], bands = np_dtw.shape[1], w=[.5,.5], sigma=1, learning_rate=0.3,
                   random_seed=42,gl_const="sakoe_chiba", scr=60)
    som.random_weights_init(np_dtw)
    som.train_batch(np_dtw, 100, verbose=True)
    print('EXPORT THE RESULTS')
    with open(f'//data/ksa/04_Data_Preprocessing/training-test/{kdprov}/som_training_yy.pkl','wb') as file:
        pickle.dump(som,file)
    print('DONE')
    print('===============================================')
    

def main(kdprov):
    som_run(kdprov)

if __name__ == "__main__":
    kdprov = sys.argv[1]
    main(kdprov)
