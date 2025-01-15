import pandas as pd
import warnings
import gc
import json
warnings.filterwarnings('ignore')
gc.collect()
#%run /home/jovyan/work/riskModel/a0001/copy_0822/utils/connect_sql_function.py
#%run /home/jovyan/work/riskModel/a0001/copy_0822/utils/variable_function.py

json_file_path = 'var_dict.json'
with open(json_file_path, 'r', encoding='utf-8') as file:
    var_dict = json.load(file)

def preprocess_variables(dt, dtid, col_name, date_col, m, params): 
    # step1: check date type
    dt['index_date'] = pd.to_datetime(dt['index_date'])
    dt[date_col] = pd.to_datetime(dt[date_col])

    # step2: cal diff
    follow_up = params["follow_up"]
    dt['diff'] = (dt['index_date'] - dt[date_col]).dt.days
    dt = dt[(0 < dt['diff']) & (dt['diff'] < follow_up)]
    dt = dt.sort_values(by='diff')
    # calculate weight
    dt['weight'] = (follow_up - dt['diff']) / follow_up
    
    # check na id in range true: 9999 false: -9999
    na_id = list(set(dtid['id']) - set(dt['id']))
    
    method = method_functions[m]
    params["dt"] = dt
    params["col_name"] = col_name    
    dt = method(**params)
    dtid = dtid.merge(dt, on = 'id', how = 'left')

    # fillna
    dtid.loc[dtid['id'].isin(na_id) & dtid[col_name].isna(), col_name] = "9999"
    dtid.loc[~dtid['id'].isin(na_id) & dtid[col_name].isna(), col_name] = "-9999"
    
    params["dt"] = pd.DataFrame()            
    del params
    gc.collect()

    return dtid

def get_data(dt_id, var_dict):
    '''
    dt_id: id, index_date
    '''
    for tb_name, tb_content in var_dict.items():
        id_col = tb_content["common_params"]["id_col"]
        date_col = tb_content["common_params"]["date_col"]
        
        for var_name, table_info in tb_content["variables"].items():
            
            # step1: get_data()
            var_type = table_info['var_type']
            cols = list(set(table_info['columns']))
            c_var = [x for x in cols if x not in ["index_date"]][0]
            
            print(f"Table(col):{tb_name}({var_name})")

            get_data_params = {
                                "table_name" : tb_name, # str
                                "col_name" : [c_var] + [id_col] , # list 
                                "cond" : '', # str
                                "id_col" : id_col, # str
                                "date_col" : [date_col] # list
            }
            
            df_var = get_data(**get_data_params)
            
            # step2: merge id data 
            df_var.rename(columns={id_col:"id"},inplace = True)
            df_var = df_var.merge(dt_id[['id','index_date']], on = 'id', how = 'inner') 
            
            # data_type transform:
            if var_type == "cont" or var_type == "ord":
                df_var = pd.to_numeric(df_var, errors='coerce').astype('float32')

            # step3: select calculate_method or not
            if table_info["c_m"] != {''}:
                for c, p_c in table_info["c_m"].items():
                    cal_fun = cal_functions[c]
                    p_c["dt"] = df_var
                    p_c["col_name"] = c_var
                    df_var = cal_fun(**p_c)
                    
            # step4 / step5: transfrom: todo: params flixible
            for m, params in table_info["methods"].items():
        
                dt_id = preprocess_variables(df_var, dt_id, c_var, date_col, m, params)
                dt_id = dt_id.rename(columns={c_var:f"{var_name}_{m}"})
                print(dt_id[f"{var_name}_{m}"].nunique())
            
            del df_var
            gc.collect()
            
        print("all preprocess done")

    return dt_id
