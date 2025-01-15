import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

# step3: calculate_functions: 
def calculate_isin(dt, col_name, name_list):
    dt = dt[dt[col_name].isin(name_list)]
    return dt

def calculate_diff_date(dt, col_name, col_name2):
    dt[col_name] = pd.to_datetime(dt[col_name])
    dt[col_name2] = pd.to_datetime(dt[col_name2])
    dt[col_name] = (dt[col_name2] - dt[col_name] ).dt.days // 365
    return dt 

cal_functions = {
    "isin": calculate_isin,
    "diff_date": calculate_diff_date
}

# step4: method_functions: 
def fetch_last_data(dt, col_name):
    dt = dt.groupby(['id']).last().reset_index()
    return dt[['id',col_name]]

def fetch_exist_data(dt, col_name):
    if dt.empty:
        return pd.DataFrame(columns=['id', col_name])
    grouped = dt.dropna(subset=[col_name]).groupby('id')[col_name].count()
    result = grouped.gt(0).astype(int).reset_index(name=col_name)
    return result

def process_occurrence(dt, col_name):    
    dt = (dt.groupby('id')[col_name].count().reset_index(name=f"{col_name}_occurrence"))
    return dt

def process_last_weighted(dt, col_name):
    dt['weighted_value'] = dt['weight'] * dt[col_name]
    dt = dt.groupby('id').last().reset_index()
    dt = dt[['id', 'weighted_value']].rename(
        columns={'weighted_value': f"{col_name}_last_weighted"}
    )
    return dt

def process_average(dt, col_name):
    dt = dt.groupby('id')[col_name].mean().reset_index(name=f"{col_name}_mean")
    return dt

def process_weighted_average(dt, col_name):
    dt['weighted_value'] = dt[col_name] * dt['weight']
    dt = (
        dt.groupby('id')['weighted_value'].sum()
        / dt.groupby('id')['weight'].sum()
    ).reset_index(name=f"{col_name}_weighted_avg")
    return dt

def process_difference(dt, col_name, date_col):
    dt = dt.sort_values(by=['id', date_col])
    dt[f"{col_name}_difference"] = dt.groupby('id')[col_name].shift(-1) - dt[col_name]
    dt[f"{col_name}_difference"] = dt[f"{col_name}_difference"].where(dt.groupby('id')[col_name].transform('size') > 1, np.nan)
    return dt.reset_index(drop=True)[['id',f"{col_name}_difference"]]

def process_std(dt, col_name):
    dt = dt.groupby('id')[col_name].std().reset_index(name=f"{col_name}_std")
    return dt

# new functions: process_regression
def process_regression(dt, col_name):
    mean_X = dt['diff'].mean()
    mean_Y = dt[col_name].mean()
    dt['beta1_manual'] = (dt['diff'] - mean_X) * (dt[col_name] - mean_Y)
    beta1_numerator = dt.groupby('id')['beta1_manual'].sum()
    beta1_denominator = dt.groupby('id').apply(lambda group: ((group['diff'] - mean_X) ** 2).sum())
    beta1 = beta1_numerator / beta1_denominator
    results = beta1.reset_index(name=f"{col_name}_regression")
    return results

def calculate_weighted_sum(dt, col_name):
    dt['weighted_value'] = dt[col_name] * dt['weight']
    result = dt.groupby('id')['weighted_value'].sum().reset_index(name=f"{col_name}_weighted")
    return result

method_functions = {
    "last": fetch_last_data,
    "id_exist": fetch_exist_data,
    "occurrence": process_occurrence,
    "last_weighted": process_last_weighted,
    "average": process_average,
    "weighted_average": process_weighted_average,
    "difference": process_difference,
    "std": process_std,
    "regression": process_regression,
    "weighted_sum": calculate_weighted_sum
}
