import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
# transfer str to int 


# step3: calculate_functions                 
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

# step4: method_functions
def fetch_last_data(dt, col_name):
    dt = dt.groupby(['id']).last().reset_index()
    return dt[['id',col_name]]

def fetch_exist_data(dt, col_name):
    if dt.empty:
        return pd.DataFrame(columns=['id', col_name])
    dt = dt.groupby('id').apply(
        lambda dt: int(dt[col_name].notnull().any())
    ).reset_index(name = f"{col_name}")
    return dt

def process_occurrence(dt, col_name):    
    dt = (dt.groupby('id')[col_name].count().reset_index(name=f"{col_name}_occurrence"))
    return dt

# new functions
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
    dt = dt.groupby('id').apply(
        lambda x: x.sort_values(date_col).iloc[-1][col_name]
        - x.sort_values(date_col).iloc[-2][col_name]
        if len(x) > 1
        else np.nan
    )
    dt = dt.reset_index(name=f"{col_name}_difference")
    return dt

def process_std(dt, col_name):
    dt = dt.groupby('id')[col_name].std().reset_index(name=f"{col_name}_std")
    return dt

def process_regression(dt, col_name):
    results = []
    for _, group in dt.groupby('id'):
        if len(group) < 2:
            results.append((group['id'].iloc[0], np.nan))
        else:
            X = group['diff'].values.reshape(-1, 1)
            y = group[col_name].values
            model = LinearRegression().fit(X, y)
            results.append((group['id'].iloc[0], model.coef_[0]))
    results = pd.DataFrame(results, columns=['id', f"{col_name}_regression"])
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
