import warnings
import gc
gc.collect()
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import pymssql

# check 有哪些資料庫:25 TABLES
def get_db_name(v):
    
    """
    parmasfro: 
    v(str): table name
    
    將不同資料庫做標準化
    
    "TRAN_PERSON_DATA", "DRUGABUSE"
    
    return 統一的資料庫(str)
    """
    
    db_map = {
    "112MentalCareData": ["CSSM_REPORT", "CSSM_VISIT"],
    "112PsycheCare": ["PSYCHOSIS", "CSSP_VISIT", "CSSP_CARE_RSHIP", "DSPC_VISIT",
                   "DSPC_VISIT_FAMILY", "DSPC_VISIT_RISK", "TRANSFER_NORMAL",
                   "NOTICE", "CSSP_ESCORT", "ICD_PSY_DATA", "TRAN_PERSON_DATA",
                   "DPSC", "MST_DAILY", "API_ABSTINENCE_DATA", "CSSM_DAILY", "PENALTY_DATA",
                   "API_MOJ_MJAC", "API_MOJ_CCII", "API_ICF_DATA",
                   "API_SNNv2CSSM_S1","D_DSPC_VISIT_NEED"], # new
    "formsip" : ["DRCASEINFO", "DRUGABUSE","DRCASETRACK","DRCURETRACK"], # new
    "Staging_ACTSDB" : ["ACT_Addiction_Case","ACT_Addiction_Substance","ACT_Case_Addiction_Substance"],    
    "Staning_精神照護系統語意資料庫":["個案紀錄表","訪視單"]}

    for i, j in db_map.items():
        if v in j:
            n = i
    
    return(n)

# get sql data
def get_sql_data(table_name, col_name='all', cond=''):
    '''
    parmas:
    col_name(list): 哪些欄位名稱
    cond: query 條件式
    
    step1: create query
    step2: get db name
    step3: connect sql
    
    return data frame
    '''
    
    # create query
    if col_name != 'all':
        c_name = ','.join(col_name) if len(col_name) > 1 else col_name[0] 
    else:
        c_name = '*'
    query = "select " + c_name + " from " + table_name
    query = query + ' ;' if cond == '' else query + ' WHERE ' + cond + ' ;'
    
    # get db name
    db_name = get_db_name(table_name)

    # connect sql 
    conn = pymssql.connect(server='203.65.96.54', user='iisiuser', password='iis1@Admin', database=db_name)
    cursor = conn.cursor(as_dict=True)
    
    # run
    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()
    data = pd.DataFrame(data)
        
    return(data)

def get_data(table_name, col_name, cond='', id_col='', date_col=''):
    '''
    step 1. check key
        1. 欄位 : coln + key_dict[[table_name]]
        2. 去重複

    step 2. get data
        get_sql_data()

    step 3, table_name in names(merge_dict) 

    step 4. 串接main
        1. 取得主檔 : merge_dict[[table_name]][['main_table']]
        2. get_sql_data(主檔, coln=key_dict[[主檔]])
        3. merge by merge_dict[[table_name]][['merge_col']]

    step 4. 修改日期
    '''
    key_dict = {
        # CSSM
        "CSSM_REPORT" : ['SID','SUICIDEDATE'],    
        "CSSM_VISIT" : ['SID','VISITDATE'],    
        # Staging_ACTSDB
        "ACT_Addiction_Case" : ['Acts_Sn', 'Apply_Date'], 
        "ACT_Addiction_Substance" : [], 
        "ACT_Case_Addiction_Substance" : ['Acts_Sn'],    
        # Staging_formsip
        "DRCASEINFO" : ['PID', 'serviceDate'],     
        "DRUGABUSE" : ['PID','CRT_DATE'],     
        "DRCASETRACK" : ['PID', 'trackDate'],     
        "DRCURETRACK" : ['PID','trackDate'],   
        # Staging_PsycheCare
        "PSYCHOSIS" : ['PS_PID','PS_RDATE'],     
        # "PSYCHOSIS" : ['PS_PID'],     

        "CSSP_VISIT" : ['V_V1SEQ', 'V_PID', 'V_TDATE'],     
        "CSSP_CARE_RSHIP" : ['CR_PID','V_V1SEQ'],     
        "CSSP_ESCORT" : ['ES_PID', 'ES_HDATE'],   
        "DSPC_VISIT" : ["_PS_PID_NOUSE", "V_DATE", "_VID_NOUSE"],     
        "DSPC_VISIT_FAMILY" : ["_DSPC_VISIT_ID_NOUSE","_PS_PID_NOUSE"],     
        "DSPC_VISIT_NEED" : ["_DSPC_VISIT_ID_NOUSE", "_PS_PID_NOUSE"],    
        "DSPC_VISIT_RISK" : ["_DSPC_VISIT_ID_NOUSE", "_PS_PID_NOUSE"],     
        "TRANSFER_NORMAL" : ["IDN","NOTICE_TIME"],     
        "NOTICE" : ["IDN","NOTICE_TIME"],     
        "ICD_PSY_DATA" : ['PS_PID','CHECKDATE'],    
        "TRAN_PERSON_DATA" : ['pid','servicedate'],
        "DPSC" : ['PS_PID','STARTDATE'],
        "MST_DAILY" : ['SID','AcceptCaseDate'],    
        "API_ABSTINENCE_DATA" : ['PID','AcceptCaseDate'],
        "CSSM_DAILY" : [],    
        "PENALTY_DATA" : [],    
        "API_MOJ_MJAC" : [],    
        "API_MOJ_CCII" : ['CASE_PID','EF_JUDT'],    
        "API_ICF_DATA" : ['ID','APPRAISAL_DATE'],    
        "API_SNNv2CSSM_S1" : ['SID','REPORTDATE','CSSM_DATE'],    
    }
    sub_table_dict = {
        "ACT_Case_Addiction_Substance" : {
            "main_table" : "ACT_Addiction_Case",
            "merge_col_main" : ["Acts_Sn"],
            "merge_col_sub" : ["Acts_Sn"]
        },
        "CSSP_CARE_RSHIP" : {
            "main_table" : "CSSP_VISIT",
            "merge_col_main" : ['V_PID',"V_V1SEQ"],
            "merge_col_sub" : ["CR_PID","V_V1SEQ"]
        },
        "DSPC_VISIT_FAMILY" : {
            "main_table" : "DSPC_VISIT",
            "merge_col_main" : ["_VID_NOUSE", "_PS_PID_NOUSE"],
            "merge_col_sub" : ["_DSPC_VISIT_ID_NOUSE", "_PS_PID_NOUSE"]
        },
        "DSPC_VISIT_NEED" : {
            "main_table" : "DSPC_VISIT",
            "merge_col_main" : ["_VID_NOUSE", "_PS_PID_NOUSE"],
            "merge_col_sub" : ["_DSPC_VISIT_ID_NOUSE", "_PS_PID_NOUSE"]
        },
        "DSPC_VISIT_RISK" : {
            "main_table" : "DSPC_VISIT",
            "merge_col_main" : ["_VID_NOUSE", "_PS_PID_NOUSE"],
            "merge_col_sub" : ["_DSPC_VISIT_ID_NOUSE", "_PS_PID_NOUSE"]
        }
    }
    rename_table_list = ["DSPC_VISIT_FAMILY","DSPC_VISIT_NEED","DSPC_VISIT_RISK"]
    
    date_table_list = ['API_ICF_DATA', 'API_MOJ_CCII', 'API_ICF_SNNv2CSSM_S1']
    
    col_name = [x for x in col_name if x not in key_dict[table_name]]
    col_name = col_name + key_dict[table_name]
    
    if col_name == []: 
        df = get_sql_data(table_name=table_name,
                          cond=cond)        
    else:
        df = get_sql_data(table_name=table_name,
                          col_name=col_name,
                          cond=cond)

    if(table_name in list(sub_table_dict.keys())):
        if(table_name in rename_table_list):
            # rename
            date_df = get_sql_data(table_name=sub_table_dict[table_name]['main_table'],
                                   col_name=key_dict[sub_table_dict[table_name]['main_table']])
            df = df.merge(date_df,
                          left_on=sub_table_dict[table_name]["merge_col_sub"],
                          right_on=sub_table_dict[table_name]["merge_col_main"],
                          how='inner')             
        else:   
            date_df = get_sql_data(table_name=sub_table_dict[table_name]['main_table'],
                                   col_name=key_dict[sub_table_dict[table_name]['main_table']])
            df = df.merge(date_df,
                          left_on=sub_table_dict[table_name]["merge_col_sub"],
                          right_on=sub_table_dict[table_name]["merge_col_main"],
                          how='inner')  
        
    # clean id col
    if id_col != '':
        df[id_col] = df[id_col].str.replace('Encrypted', '')
        df[id_col] = df[id_col].str.replace('-', '')
    
    # change to datetime
    if date_col != '':
        if table_name in date_table_list:
            for i in date_col:
                df['new_date'] = pd.to_numeric(df[i]) + 19110000
                df['new_date'] = np.where((df['new_date'] < 19110000) | (df['new_date'] > 20300000), np.nan, df['new_date'])
                df['new_date'] = df['new_date'].apply(str)
                df['new_date'] = df['new_date'].str.slice(0, 8)
                df[i] = pd.to_datetime(df['new_date'], errors='coerce').dt.normalize() #統一格式
                df = df.drop(['new_date'], axis=1)
        else:
            for i in date_col:
                df[i] = pd.to_datetime(df[i], errors="coerce").dt.normalize() #統一格式
    
    return df
            