import pandas as pd
import multiprocessing as mp


conditions = {
    1: 'V1',
    2: 'V2',
    3: 'G',
    6: 'P',
    7: 'NP',
    8: 'NV',
    12: 'NA'
}

def relabel_data(sub_data, id_col, nth_col, obs_col):
    for obs_value, class_value in conditions.items():
        sub_data.loc[sub_data[obs_col] == obs_value, 'class'] = class_value

    for idx, row in sub_data[sub_data[obs_col] == 4].iterrows():
        curr_nth = row[nth_col]
        curr_idx = row[id_col]
        prev_row = sub_data[(sub_data[id_col] == curr_idx) & (sub_data[nth_col] == curr_nth - 1)]
        if not prev_row.empty and prev_row.iloc[0][obs_col] == 4:
            sub_data.at[idx, 'class'] = 'BL'
        else:
            sub_data.at[idx, 'class'] = 'H'

    for idx, row in sub_data[sub_data[obs_col] == 5].iterrows():
        curr_nth = row[nth_col]
        curr_idx = row[id_col]
        next_row = sub_data[(sub_data[id_col] == curr_idx) & (sub_data[nth_col] == curr_nth + 1)]
        if not next_row.empty and next_row.iloc[0][obs_col] == 5:
            sub_data.at[idx, 'class'] = 'BL'
        else:
            sub_data.at[idx, 'class'] = 'PL'

    return sub_data

def process_batch(batch, id_col, nth_col, obs_col):
    return relabel_data(batch, id_col, nth_col, obs_col)

def batch_data(data, id_col, batch_size):
    unique_ids = data[id_col].unique()
    for i in range(0, len(unique_ids), batch_size):
        batch_ids = unique_ids[i:i + batch_size]
        yield data[data[id_col].isin(batch_ids)]

def parallel_relabeling(data, id_col='id_x', nth_col='nth', obs_col='obs', batch_size=1000):
    
    with mp.Pool(mp.cpu_count()) as pool:
        batches = batch_data(data, id_col, batch_size)
        results = pool.starmap(process_batch, [(batch, id_col, nth_col, obs_col) for batch in batches])
    return pd.concat(results)