import pandas as pd
import math
import datetime
import gc
import os
import numpy as np
import glob
import random
import configparser
import pymysql
import mysql.connector
from sqlalchemy import create_engine
from pandas import DataFrame
from dateutil.relativedelta import relativedelta, MO
import collections
import json as JSON 
import dill as pickle
import msoffcrypto
from pathlib import WindowsPath
import collections
import io
import scorecardpy as sc
import pandasql as ps
import re
from itertools import groupby
from collections import Counter
import warnings
warnings.filterwarnings('ignore')
import sys
from threading import Thread
import time
from ctypes import cdll, CDLL, util
import platform
print("platform:",platform.system()) 
if ("Darwin" == platform.system()):
    cdll.LoadLibrary("/usr/lib/libc.dylib")
    libc = CDLL("libc.dylib")
elif ("Linux" == platform.system()):
    cdll.LoadLibrary("libc.so.6")
    libc = CDLL("libc.so.6")
elif ("Windows" == platform.system()):
    path_libc = util.find_library("cdll.msvcrt")
    libc = CDLL(path_libc)
else:
    cdll.LoadLibrary("libc.so.6")
    libc = CDLL("libc.so.6")


running_flag = True
class heartbeat(Thread):
    def __init__(self):
        Thread.__init__(self)
    def run(self):
        while running_flag:
            time.sleep(5)#Print Every 5 Sec.
            if datetime.datetime.now().second > 0 and datetime.datetime.now().second <= 5:
                print("Running ... Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                sys.stdout.flush()
def lower_keys(x):
   if isinstance(x, list):
     return [lower_keys(v) for v in x]
   elif isinstance(x, dict):
     return dict((k.lower(), lower_keys(v)) for k, v in x.items())
   else:
     return x
def result(js):
    if isinstance(js, str):
        js =JSON.loads(js)
    else:
        js =js
    return js
def output(js):
    if isinstance(js, dict):
        js=JSON.dumps(js,indent=4)
    else:
        js =js
    return js
def div(denom,divisor,result):
    if divisor in ["", 0, np.NaN, None]:
        result=result
    else:
        result=float(double.Decimal(str(float(denom)))/double.Decimal(str(float(divisor))))
    return result

pth="./credit review/"
configParser = configparser.RawConfigParser()
configFilePath = r'./config.txt'
configParser.read(configFilePath)
dbHost = configParser.get('db-config', 'db.host')
dbName = configParser.get('db-config', 'db.name')
dbUser = configParser.get('db-config', 'db.user')
dbPass = configParser.get('db-config', 'db.pass')
dbPortStr = configParser.get('db-config', 'db.port')
dbPort = 3306
if dbPortStr != None and dbPortStr != '':
    dbPort = int(dbPortStr)

engine = create_engine("mysql+pymysql://{user}:{pw}@{host}:{port}/{db}".format(
    user=dbUser, pw=dbPass, host=dbHost, port=dbPort, db=dbName), pool_recycle=dbPort, pool_pre_ping=True)
try:
    #executedate = '2021-01-03'
    #tod = datetime.datetime.strptime(executedate, "%Y-%m-%d")
    if len(sys.argv) > 1:
        print("###############################")
        executedate = sys.argv[1]
        tod = datetime.datetime.strptime(executedate, "%Y-%m-%d")
        print("execute date: ", tod)
    else:
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
        print("Parameter executedate is missing")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        sys.exit(1)
        
    rerun_flag = 'no'
    if len(sys.argv) > 2:
        rerun_flag = sys.argv[2].lower()
    print("rerun flag: ", rerun_flag)
    
    thr1 = heartbeat()
    thr1.start()
    
    ###########################################################################
    #####2_1_factor_wcd##START####
    ###########################################################################
    
    list_file=glob.glob("""%scar_depreciation*.xlsx""" % (pth))
    list_file.sort()
    car_depreciation=pd.read_excel(list_file[-1], sheet_name='group')
    car_depreciation['brand_std'] = car_depreciation['brand_std'].str.lower()
    
    def extract_data_for_bscore(today, obs_period=6):
        if today.day == 1:
            
            list_date = [today + relativedelta(months=-m, day=31) for m in range(1,obs_period+1)]
            sql_script = """
                SELECT  s.pos_dt, s.acc_no, s.id_no, s.mob, 
                        s.out_0m_b, s.out_1m_b, s.out_2m_b, s.out_3m_b, s.out_4m_b, s.out_5m_b, 
                        s.limit, s.interest_rate, s.installment_amount, s.first_payment_default_flag, 
                        s.first_three_payments_default_flag, s.dlq_class, s.dpd,
                        r.verified_monthly_income as monthly_income,
                        coalesce( cast(j.appraisal_price as SIGNED),0) as caractualprice,
                        j.car_brand as carBrand, 
                        j.car_age as carAge,
                        j.new_car_flag
                FROM `std_m_ldr_acc` as s
                LEFT JOIN `flexcube_ldr` as r
                ON (s.acc_no = r.acc_no and s.pos_dt = r.pos_dt )
                LEFT JOIN `re_json1_car_loan` as j
                ON r.con_no = j.app_no
                WHERE s.pos_dt = date('%s') 
                AND s.product = 'car_loan'
                AND s.app_source = 'CUP_YIXIN'
            """ % (list_date[0].strftime("%Y-%m-%d") )
            print(sql_script)
            curr = engine.execute(sql_script)
            ldr_all = DataFrame(curr.fetchall())
            print("** Result rows: '%s'"% len(ldr_all.index))
            if len(ldr_all.index) > 0:
                ldr_all.columns = curr.keys() 
                ldr_all = ldr_all.sort_values(by=['acc_no']).reset_index(drop=True)
                ldr_all = ldr_all.drop_duplicates(subset=['acc_no'])
                #for col in ldr_all.columns:
                #    print("column name %s" % col)
            return ldr_all
    
    def out_end(x0, r, i, t):
        if t < 1:
            return x0
        else:
            for n in range(t):
                xn = x0 + x0*r/12 - i
                x0 = xn
            return xn
    
    def act_to_sch(act, sch):
        if act == 0:
            return 999
        else:
            return round(act / sch, 3)
    
    def group_act_to_sch(x):
        if x >= 1.03:
            return 2
        elif x >= 1.005:
            return 1
        elif x >= 0.995:
            return 0
        elif x >= 0.99:
            return -1
        elif x < 0.99:
            return -2
        else:
            return 0
    
    def act_to_sched(df):        
        df_last = df[['acc_no', 'mob', 'out_0m_b', 'out_1m_b', 'out_2m_b', 'out_3m_b', 'out_4m_b', 'out_5m_b', 'limit', 'interest_rate', 'installment_amount']]
        df_last['balance_end_0m_b'] = df_last.apply(lambda row: out_end(row.limit, row.interest_rate, row.installment_amount, row.mob-0), axis = 1) 
        df_last['balance_end_1m_b'] = df_last.apply(lambda row: out_end(row.limit, row.interest_rate, row.installment_amount, row.mob-1), axis = 1) 
        df_last['balance_end_2m_b'] = df_last.apply(lambda row: out_end(row.limit, row.interest_rate, row.installment_amount, row.mob-2), axis = 1) 
        df_last['balance_end_3m_b'] = df_last.apply(lambda row: out_end(row.limit, row.interest_rate, row.installment_amount, row.mob-3), axis = 1) 
        df_last['balance_end_4m_b'] = df_last.apply(lambda row: out_end(row.limit, row.interest_rate, row.installment_amount, row.mob-4), axis = 1) 
        df_last['balance_end_5m_b'] = df_last.apply(lambda row: out_end(row.limit, row.interest_rate, row.installment_amount, row.mob-5), axis = 1) 
        df_last['act_to_sched_out_0m_b'] = df_last.apply(lambda row: act_to_sch(row.out_0m_b, row.balance_end_0m_b), axis = 1)
        df_last['act_to_sched_out_1m_b'] = df_last.apply(lambda row: act_to_sch(row.out_1m_b, row.balance_end_1m_b), axis = 1)
        df_last['act_to_sched_out_2m_b'] = df_last.apply(lambda row: act_to_sch(row.out_2m_b, row.balance_end_2m_b), axis = 1)
        df_last['act_to_sched_out_3m_b'] = df_last.apply(lambda row: act_to_sch(row.out_3m_b, row.balance_end_3m_b), axis = 1)
        df_last['act_to_sched_out_4m_b'] = df_last.apply(lambda row: act_to_sch(row.out_4m_b, row.balance_end_4m_b), axis = 1)
        df_last['act_to_sched_out_5m_b'] = df_last.apply(lambda row: act_to_sch(row.out_5m_b, row.balance_end_5m_b), axis = 1)
        df_last['group_0m_b'] = df_last['act_to_sched_out_0m_b'].apply(group_act_to_sch)
        df_last['group_1m_b'] = df_last['act_to_sched_out_1m_b'].apply(group_act_to_sch)
        df_last['group_2m_b'] = df_last['act_to_sched_out_2m_b'].apply(group_act_to_sch)
        df_last['group_3m_b'] = df_last['act_to_sched_out_3m_b'].apply(group_act_to_sch)
        df_last['group_4m_b'] = df_last['act_to_sched_out_4m_b'].apply(group_act_to_sch)
        df_last['group_5m_b'] = df_last['act_to_sched_out_5m_b'].apply(group_act_to_sch)
        df_last['pattern_ratio'] = (df_last['group_5m_b'].astype(str) + '-' + df_last['group_4m_b'].astype(str) + '-' +
                                    df_last['group_3m_b'].astype(str) + '-' + df_last['group_2m_b'].astype(str) + '-' +
                                    df_last['group_1m_b'].astype(str) + '-' + df_last['group_0m_b'].astype(str))
        
        df_last = df_last[['acc_no', 'group_0m_b', 'group_1m_b', 'group_2m_b', 'group_3m_b', 'group_4m_b', 'group_5m_b', 'pattern_ratio', 'act_to_sched_out_0m_b', 'act_to_sched_out_1m_b','act_to_sched_out_2m_b','act_to_sched_out_3m_b','act_to_sched_out_4m_b','act_to_sched_out_5m_b']]
        
        return df_last
    
    def behavioral_factors(df, today):       
        balance = act_to_sched(df)
        df_last = df.copy()
        df5 = pd.merge(df_last, balance, on=['acc_no'], how='left')
        df5['monthly_repayment_(PBOC)'] = df5['installment_amount']
        df5['current_debt_burden'] = df5['monthly_repayment_(PBOC)'] / df5['monthly_income'] #df5['salary']
        df5['carBrand'] = df5['carBrand'].fillna('Other')
        df5['carBrand'] = df5['carBrand'].apply(lambda x: x.split()[0].lower())
        df5['balance_F1Y'] = df5.apply(lambda row: out_end(row.out_0m_b, row.interest_rate, row.installment_amount, 12), axis = 1) 
        df5['balance_F2Y'] = df5.apply(lambda row: out_end(row.out_0m_b, row.interest_rate, row.installment_amount, 24), axis = 1) 
        df5['balance_F3Y'] = df5.apply(lambda row: out_end(row.out_0m_b, row.interest_rate, row.installment_amount, 36), axis = 1) 
        df6 = pd.merge(df5, car_depreciation['brand_std'], left_on='carBrand', right_on='brand_std', how='left')
        df6['brand_std'] = df6['brand_std'].fillna('other')
        df7 = pd.merge(df6, car_depreciation, on=['brand_std'], how='left')
        df7['carAge'] = df7['carAge'].fillna(0)
        df7['car_age_current_year'] = df7['carAge'] + df7['mob']/12
        df7['car_age_F1Y_year'] = df7['car_age_current_year'] + 1
        df7['car_age_F2Y_year'] = df7['car_age_current_year'] + 2
        df7['car_age_F3Y_year'] = df7['car_age_current_year'] + 3
        df7['car_age_ori'] = df7['carAge'].apply(lambda x: str(int(x))) + 'Y0M'
        df7['car_age_current'] = (df7['carAge'] + df7['mob'].apply(lambda x: int(x/12) if x>0 else 0)).apply(lambda x: str(int(x))) + 'Y' + df7['mob'].apply(lambda x: str(x % 12) if x>0 else str(0)) + 'M'
        df7['car_age_F1Y'] = df7['car_age_current'].apply(lambda x: str(1 + int(re.findall('\d+',x)[0]))) + df7['car_age_current'].apply(lambda x: re.findall('Y.+M',x)[0])
        df7['car_age_F2Y'] = df7['car_age_current'].apply(lambda x: str(2 + int(re.findall('\d+',x)[0]))) + df7['car_age_current'].apply(lambda x: re.findall('Y.+M',x)[0])
        df7['car_age_F3Y'] = df7['car_age_current'].apply(lambda x: str(3 + int(re.findall('\d+',x)[0]))) + df7['car_age_current'].apply(lambda x: re.findall('Y.+M',x)[0])
        
        df7.loc[df7['carAge']>=10, 'car_age_ori'] = '10Y0M'
        df7.loc[df7['car_age_current_year']>=10, 'car_age_current'] = '10Y0M'
        df7.loc[df7['car_age_F1Y_year']>=10, 'car_age_F1Y'] = '10Y0M'
        df7.loc[df7['car_age_F2Y_year']>=10, 'car_age_F2Y'] = '10Y0M'
        df7.loc[df7['car_age_F3Y_year']>=10, 'car_age_F3Y'] = '10Y0M'

        for i in range(df7.shape[0]):
            df7.loc[i, 'car_price_current'] = df7.loc[i, 'caractualprice'] * df7.loc[i, df7.loc[i, 'car_age_current']] / df7.loc[i, df7.loc[i, 'car_age_ori']]
            df7.loc[i, 'car_price_F1Y']     = df7.loc[i, 'caractualprice'] * df7.loc[i, df7.loc[i, 'car_age_F1Y']] / df7.loc[i, df7.loc[i, 'car_age_ori']]
            df7.loc[i, 'car_price_F2Y']     = df7.loc[i, 'caractualprice'] * df7.loc[i, df7.loc[i, 'car_age_F2Y']] / df7.loc[i, df7.loc[i, 'car_age_ori']]
            df7.loc[i, 'car_price_F3Y']     = df7.loc[i, 'caractualprice'] * df7.loc[i, df7.loc[i, 'car_age_F3Y']] / df7.loc[i, df7.loc[i, 'car_age_ori']]

        df8 = df7.copy()
        df8['LTV_current'] = df8['out_0m_b'] / df8['car_price_current']
        df8['LTV_F1Y'] = df8['balance_F1Y'] / df8['car_price_F1Y']
        df8['LTV_F2Y'] = df8['balance_F2Y'] / df8['car_price_F2Y']
        df8['LTV_F3Y'] = df8['balance_F3Y'] / df8['car_price_F3Y']
        df8['act_to_sched_out_current'] = df8['act_to_sched_out_0m_b']

        df9 = df8.copy()
        df9['LTV_F1Y'] = df9['LTV_F1Y'].apply(lambda x: max(0,x))
        df9['LTV_F2Y'] = df9['LTV_F2Y'].apply(lambda x: max(0,x))
        df9['LTV_F3Y'] = df9['LTV_F3Y'].apply(lambda x: max(0,x))
        df9.loc[df9.LTV_current == np.inf, 'LTV_current'] = np.nan
        df9.loc[df9.LTV_F1Y == np.inf, 'LTV_F1Y'] = np.nan
        df9.loc[df9.LTV_F2Y == np.inf, 'LTV_F2Y'] = np.nan
        df9.loc[df9.LTV_F3Y == np.inf, 'LTV_F3Y'] = np.nan

        df9.rename(columns={
            'rccr':'rccr_a_sc'
        }, inplace=True)
        colnamelower = [x.lower() for x in df9.columns]
        df9.columns = colnamelower
        df9.drop(['0y0m','0y1m','0y2m','0y3m','0y4m','0y5m','0y6m','0y7m','0y8m','0y9m','0y10m','0y11m','1y0m','1y1m','1y2m','1y3m','1y4m','1y5m','1y6m','1y7m','1y8m','1y9m','1y10m','1y11m','2y0m','2y1m','2y2m','2y3m','2y4m','2y5m','2y6m','2y7m','2y8m','2y9m','2y10m','2y11m','3y0m','3y1m','3y2m','3y3m','3y4m','3y5m','3y6m','3y7m','3y8m','3y9m','3y10m','3y11m','4y0m','4y1m','4y2m','4y3m','4y4m','4y5m','4y6m','4y7m','4y8m','4y9m','4y10m','4y11m','5y0m','5y1m','5y2m','5y3m','5y4m','5y5m','5y6m','5y7m','5y8m','5y9m','5y10m','5y11m','6y0m','6y1m','6y2m','6y3m','6y4m','6y5m','6y6m','6y7m','6y8m','6y9m','6y10m','6y11m','7y0m','7y1m','7y2m','7y3m','7y4m','7y5m','7y6m','7y7m','7y8m','7y9m','7y10m','7y11m','8y0m','8y1m','8y2m','8y3m','8y4m','8y5m','8y6m','8y7m','8y8m','8y9m','8y10m','8y11m','9y0m','9y1m','9y2m','9y3m','9y4m','9y5m','9y6m','9y7m','9y8m','9y9m','9y10m','9y11m','10y0m'],axis=1,inplace=True)

        ym = []
        for y in range(11):
            for m in range(12):
                ym.append(str(y) + 'y' + str(m) + 'm')
    
        df10 = df9[[x for x in df9.columns if x not in ym]]
        return df10
    ###########################################################################
    #####2_1_factor_wcd##END####
    ###########################################################################
    
    
    ##############################################################################################
    ####2_1_1_credit_scoring###START######
    #####Monthly-acc_no####
    ##############################################################################################
    def creditscore(json_temp):
        #json_temp['b_sc']=np.NaN

        app_source=list(dict.fromkeys(json_temp['app_source']))[0].lower()
        product=list(dict.fromkeys(json_temp[json_temp['app_source']==app_source]['product']))[0].lower()
        model="b champ"

        list_file=glob.glob("""%scard %s %s %s *.xlsx""" % (pth, app_source, product, model))
        list_file.sort()
        #fmt=pd.read_excel(list_file[-1])
        #load excel with password locked 1234
        file = msoffcrypto.OfficeFile(open(list_file[-1], "rb"))
        # Use password
        file.load_key(password="1234")
        decrypted = io.BytesIO()
        file.decrypt(decrypted)
        fmt = pd.read_excel(decrypted)
        #create card from excel  
        card={}
        for i in fmt['variable'].drop_duplicates().tolist():
            card.update({i:fmt[fmt['variable']==i]})
        print("Load file: %s" % list_file[-1])
        card={k.lower(): v for k, v in card.items()}#Lower case dictionary key
        for i in card.keys():
            card[i]=card[i].applymap(lambda s:s.lower() if isinstance(s, str) else s)
        json_temp2=json_temp[(json_temp['app_source']==app_source) & (json_temp['product']==product)]
        score = sc.scorecard_ply(json_temp2, card, only_total_score=False)
        score = pd.concat([json_temp, score], axis=1, sort=False)
        score.rename(columns={'score':'b_sc'}, inplace=True)
        score["score_file"]=os.path.basename(list_file[-1])
        table_name = "%s" % (os.path.basename(list_file[-1])[5:-5])

        try:
            json_temp=score[['pos_dt','app_source','product','id_no','acc_no','mob','rccr_a_sc','b_sc','dlq_class','dpd']]
        except:
            json_temp=score[['pos_dt','app_source','product','id_no','acc_no','mob','b_sc','dlq_class','dpd']]
            json_temp['rccr_a_sc'] =np.NaN

        result_array = {}
        result_array['json_temp'] = json_temp
        result_array['score'] = score
        result_array['table_name'] = table_name
        #score.to_sql(name=table_name, con=conn, if_exists='append', index=False)#'replace'   
        return result_array
    # =============================================================================
    pickle.dump(creditscore, open('./credit review/creditscore.pkl', 'wb'))
    # =============================================================================
    
    ##############################################################################################
    ####2_1_1_credit_scoring###END######
    #####Monthly-acc_no####
    ##############################################################################################
    
    ##############################################################################################
    ####2_1_2_grading_b_sc_pick_ab_rating###START######
    #####Monthly-acc_no####
    ##############################################################################################
    #Grading b_sc
    def grad(json_temp):
        #ldr=json_temp1.copy()
        ldr=json_temp.copy()
        pth="./credit review/"
        app_source=list(dict.fromkeys(json_temp['app_source']))[0].lower()
        product=list(dict.fromkeys(json_temp[json_temp['app_source']==app_source]['product']))[0].lower()
        model="b champ"
    
    
    
        cut_off=glob.glob("""%slist app_source product*.xlsx""" % (pth))
        cut_off.sort()
        cut_off=pd.read_excel(cut_off[-1], sheet_name="Sheet1")
        cut_off.columns= cut_off.columns.str.lower() #get list to loop variable from excel

        list_file=glob.glob("""%sRetail Customer Credit Rating*.xlsx""" % (pth))
        list_file.sort()
        fmt=pd.read_excel(list_file[-1],sheet_name="""%s %s %s"""% (app_source, product, model))
        fmt.columns= fmt.columns.str.lower() #get list to loop variable from excel
        var_list_std=list(set(fmt.columns.to_list())-set(['rccr','pd','lgd','pecl']))
        var_list_std=list(set([sub.replace('_min', '').replace('_max', '') for sub in var_list_std]))
        var_list_std=[x for x in var_list_std if x in ldr.columns.to_list()]
        for i in var_list_std:
            if not isinstance(ldr.iloc[0][i], str):
                #i='ext_paoc_1'
                min_c=i+'_min'
                max_c=i+'_max'
                fmt2=fmt[['rccr',min_c, max_c]].dropna().reset_index()
                ldr["rccr_"+i]=np.NaN
                for j in fmt2.index:
                    ldr.loc[(ldr[i]>=fmt2.loc[j][min_c])&(ldr[i]<=fmt2.loc[j][max_c]),["rccr_%s"  % i]]=fmt2.loc[j,['rccr']][0]
                    ldr = ldr.replace({np.nan: None})
        #Grading Preference
        #dpd > 90 assign rating 13
        try:
            cutoff= cut_off[(cut_off['app_source']==app_source) & (cut_off['product']==product)]['cut_off'].to_list()[0]  
            ldr['rccr_acc']=ldr[['rccr_a_sc', 'rccr_b_sc']].max(axis=1)
            ldr.loc[ldr['mob'] >= cutoff, 'rccr_acc'] = ldr['rccr_b_sc']
            ldr['rccr_acc']=ldr[['rccr_acc','dpd']].apply(lambda x: 13 if x['dpd']>90 else x['rccr_acc'], axis=1)
        except:
            ldr['rccr_a_sc']=np.NaN
            ldr['rccr_acc']=ldr['rccr_b_sc']
            ldr['rccr_acc']=ldr[['rccr_acc','dpd']].apply(lambda x: 13 if x['dpd']>90 else x['rccr_acc'], axis=1)
        b_sc_acc=ldr[['pos_dt', 'id_no', 'app_source', 'product', 'acc_no', 'mob', 'rccr_a_sc', 'rccr_b_sc', 'rccr_acc']]
        #b_sc_acc.to_sql(name='b_sc_acc', con=engine, if_exists='append', index=False)#'replace'
        return b_sc_acc
    ##############################################################################################
    ####2_1_2_grading_b_sc_pick_ab_rating###END######
    #####Monthly-acc_no####
    ##############################################################################################
    #######################################
    ######Run 2_1-2_1_2 wcd #######
    #######################################

    yesterday = tod + relativedelta(days=-3)
    eom       = yesterday + relativedelta(day=31)
    yeom      = yesterday + relativedelta(months=-1, day=31)
    today     = yesterday+ relativedelta(days=+1)

    if (today.day == 1):

        if rerun_flag == "yes":
            with engine.begin() as conn:
                sql_script = (""" DELETE FROM `b_sc_acc` WHERE pos_dt = date('%s') and app_source = 'cup_yixin' and product='car_loan' """% yesterday.strftime("%Y-%m-%d") )
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                print(sql_script)
                conn.execute(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                
                sql_script = (""" DELETE FROM `cup_yixin car_loan_bscore_factor` WHERE pos_dt = date('%s') """% yesterday.strftime("%Y-%m-%d") )
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                print(sql_script)
                conn.execute(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                
                sql_script = (""" DELETE FROM `cup_yixin car_loan b champ 20210215` WHERE pos_dt = date('%s') """% (yesterday.strftime("%Y-%m-%d")) )
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                print(sql_script)
                conn.execute(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))

        json_temp = extract_data_for_bscore(today)
        print("pass extract_data_for_bscore")
        if not json_temp.empty:
            df10 = behavioral_factors(json_temp, today)
            print("pass behavioral_factors")
            json_temp = df10.copy()
            if 'borrower_english_name' in json_temp.columns:
                    json_temp=json_temp.drop(columns=['borrower_english_name'])
            
            print('before creditscore')
            creditscore = pickle.load(open("""%screditscore.pkl""" % (pth), 'rb'))
            json_temp["app_source"] = "cup_yixin"
            json_temp["product"] = "car_loan"
            json_temp1 = creditscore(json_temp)
            print('after creditscore')
            b_sc_acc = grad(json_temp1["json_temp"])

            with engine.begin() as conn:
                table_name = json_temp1['table_name']
                print('before b_sc_acc')
                b_sc_acc.to_sql(name='b_sc_acc', con=conn, chunksize=10000, if_exists='append', index=False, method='multi')#'replace'
                json_temp1["score"].to_sql(name=table_name, con=conn, chunksize=10000, if_exists='append', index=False, method='multi')#'replace'   
                print('before cup_yixin car_loan_bscore_factor')
                df10.to_sql(name='cup_yixin car_loan_bscore_factor', con=conn, chunksize=10000, if_exists='append', index=False, method='multi')#'replace'
                print("tables: b_sc_acc, cup_yixin car_loan_bscore_factor ", table_name)

    print("================================")
    print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
    print("E03_3_1_factor_cup_yixin.py: Completed")
    print("================================")
    running_flag = False
    thr1.join()
    sys.exit(0)
    
except Exception as error:
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
    strLog = "Failed to process E03_3_1_factor_cup_yixin.py {}".format(error)
    print(strLog)
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    running_flag = False
    thr1.join()
    sys.exit(1)
