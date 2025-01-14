import pandas as pd

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

method_functions = {
    "last": fetch_last_data,
    "id_exist": fetch_exist_data,
    "occurrence": process_occurrence
}
