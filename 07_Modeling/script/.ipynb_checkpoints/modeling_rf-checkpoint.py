import pandas as pd 
import numpy as np
import math
import itertools
from itertools import compress
import geopandas as gpd

import os
from glob import glob
from tqdm import tqdm
import dtw

import warnings
warnings.filterwarnings("ignore")

import seaborn as sns
import matplotlib.pyplot as plt
%matplotlib inline
from ipywidgets import interact, fixed


from tslearn.clustering import TimeSeriesKMeans
from tslearn.preprocessing import TimeSeriesScalerMeanVariance
from tslearn.utils import to_time_series_dataset


from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import confusion_matrix, accuracy_score, recall_score, precision_score
from sklearn.ensemble import RandomForestClassifier
from catboost import CatBoostClassifier
from sklearn.svm import SVC
from sklearn.model_selection import StratifiedShuffleSplit
from time import time
import math


def plot_confusion_matrix(cm, classes, title, dataset_name):
    plt.figure(figsize=(8, 8))
    
    percentages = (cm.T / cm.sum(axis=1) * 100).T
    
    plt.imshow(percentages, interpolation='nearest', cmap=plt.cm.Blues, vmin=0, vmax=100)
    plt.title(f'Confusion Matrix - {title} - {dataset_name}')
    plt.colorbar(label='Percentage')
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes, rotation=45)
    plt.yticks(tick_marks, classes)
    
    for i in range(len(classes)):
        for j in range(len(classes)):
            plt.text(j, i, f"{cm[i, j]}\n{percentages[i, j]:.1f}%", horizontalalignment='center', color='white' if percentages[i, j] > 50 else 'black')
    
    plt.tight_layout()
    plt.ylabel('True label')
    plt.xlabel('Predicted label')
    plt.show()
    plt.close()


def modeling(Y, X, nama_model, image=False):
    # Stratified split
    sss = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)

    for train_index, temp_index in sss.split(X, Y):
        X_train, X_temp = X.iloc[train_index], X.iloc[temp_index]
        Y_train, Y_temp = Y.iloc[train_index], Y.iloc[temp_index]

    sss_val = StratifiedShuffleSplit(n_splits=1, test_size=0.5, random_state=42)
    
    for test_index, val_index in sss_val.split(X_temp, Y_temp):
        X_test, X_val = X_temp.iloc[test_index], X_temp.iloc[val_index]
        Y_test, Y_val = Y_temp.iloc[test_index], Y_temp.iloc[val_index]

    models = {
        'Random Forest': RandomForestClassifier(),
        'CatBoost': CatBoostClassifier(silent=True),
    }

    results = {'Model': [], 'Pred Time': [], 'Fit Time': [], 'Mean Accuracy Train (5-fold CV)': [], 
               'Mean Accuracy Val (5-fold CV)': [], 'Accuracy Testing Pred': [], 'Precision Testing Pred': [], 'Recall Testing Pred': [],
               'Relative Acc Before':[], 'Relative Acc After':[]}
    
    accuracy_val_pred_max = 0
    
    for model_name, model in models.items():
        print(f"Processing {model_name}...{nama_model}")

        cv_scores = cross_val_score(model, X_train, Y_train, cv=5)
        fit_time_start = time()
        model.fit(X_train, Y_train)
        fit_time_end = time()

        mean_accuracy_train = cv_scores.mean()
        mean_accuracy_test = cross_val_score(model, X_test, Y_test, cv=5).mean()
        

        pred_time_start = time()
        Y_val_pred = model.predict(X_val)
        #Y_val_pred_prob = model.predict_proba(X_test)
        pred_time_end = time()

        cm = confusion_matrix(Y_val, Y_val_pred)
        class_labels = sorted(Y_val.unique())
        plot_confusion_matrix(cm, class_labels, model_name, nama_model)

        n = cm.shape[0]
        racc_before = sum(cm[i, (i+1) % n] for i in range(n)) / cm.sum()
        racc_after = sum(cm[(i+1) % n, i] for i in range(n)) / cm.sum()
        
        accuracy_val_pred = accuracy_score(Y_val, Y_val_pred)
        precision_val_pred = precision_score(Y_val, Y_val_pred, average = 'weighted')
        recall_val_pred = recall_score(Y_val, Y_val_pred, average = 'weighted')

        results['Model'].append(f'{model_name} | {nama_model}')
        results['Pred Time'].append(pred_time_end - pred_time_start)
        results['Fit Time'].append(fit_time_end - fit_time_start)
        results['Mean Accuracy Train (5-fold CV)'].append(mean_accuracy_train)
        results['Mean Accuracy Val (5-fold CV)'].append(mean_accuracy_test)
        results['Accuracy Testing Pred'].append(accuracy_val_pred)
        results['Precision Testing Pred'].append(precision_val_pred)
        results['Recall Testing Pred'].append(recall_val_pred)
        results['Relative Acc Before'].append(racc_before)
        results['Relative Acc After'].append(racc_after)
        
        if accuracy_val_pred > accuracy_val_pred_max:
            accuracy_val_pred_max = accuracy_val_pred
            model_return = model
            if model_name != 'CatBoost':
                df_pred = pd.DataFrame({"y_val": Y_val, "y_pred": Y_val_pred})
            else:
                df_pred = pd.DataFrame({"y_val": Y_val, "y_pred": Y_val_pred.flatten().tolist()})

    result_df = pd.DataFrame(results)

    return result_df, model_return, df_pred


def model_utilization(data, Y_vars, X_vars, iterasi, Category, model):
    db = pd.DataFrame()
    for idx in range(0,6):
        tpt = data.loc[data[iterasi]==idx]
        Y_pred = results_dict[f"model_return_{idx}"].predict(tpt[X_vars])
        tpt['Y_pred'] = Y_pred
        tpt['observation'] = tpt[Y_vars]
        db = pd.concat([db,tpt])
    
    list_district = list(db[Category].unique())
    result = []
    
    for i in list_district:
        temp = db.loc[db[Category] == i]
        res_temp = {}
        cm = confusion_matrix(temp['observation'].astype(str), temp['Y_pred'].astype(str))
        res_temp['district'] = i
        res_temp['accuracy_val_pred'] = accuracy_score(temp['observation'], temp['Y_pred'])
        res_temp['precision_val_pred'] = precision_score(temp['observation'], temp['Y_pred'], average='micro')
        res_temp['recall_val_pred'] = recall_score(temp['observation'], temp['Y_pred'], average='micro')
        n = cm.shape[0]
        res_temp['racc_before'] = sum(cm[i, (i+1) % n] for i in range(n)) / cm.sum()
        res_temp['racc_after'] = sum(cm[(i+1) % n, i] for i in range(n)) / cm.sum()
        res_temp['relative_acc_total'] = res_temp['accuracy_val_pred']+res_temp['racc_before']+res_temp['racc_after']
        result.append(res_temp)
    
    result_df = pd.DataFrame(result)
    
    return db, result_df

