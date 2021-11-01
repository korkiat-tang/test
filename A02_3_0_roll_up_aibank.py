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
import sqlalchemy as db
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

###########################################################################
#####3_0_roll_up_aibank (merge ldr to buyback))##START###
###########################################################################
try:
    # today = datetime.datetime.strptime('2021-06-03', "%Y-%m-%d")
    if len(sys.argv) > 1:
        print("###############################")
        executedate = sys.argv[1]
        today = datetime.datetime.strptime(executedate, "%Y-%m-%d")
        print("execute date: ", today)
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

    list_file=glob.glob("""%slist app_source product*.xlsx""" % (pth))
    list_file.sort()
    print(list_file[-1])
    if ("Linux" == platform.system()):
        libc.malloc_trim(0)
    source=pd.read_excel(list_file[-1], sheet_name="Sheet1")
    source.columns= source.columns.str.lower() #get list to loop variable from excel

    try:
        sys_lag_period = list(set(source[(source['app_source']=="ai") & (source['product']=="per_loan")]['sys_lag_period'].tolist()))[0]
    except:
        print("All app_source and product not in excel file")
        sys_lag_period = 3
    yesterday =today + relativedelta(days=-sys_lag_period)
    eom       =yesterday + relativedelta(day=31)
    yeom      =yesterday + relativedelta(months=-1, day=31)
    year      =yesterday.strftime('%Y')
    month     =yesterday.strftime('%m')
    BEG_OF_MONTH = yesterday.replace(day=1)

    if rerun_flag == "yes":
        with engine.begin() as conn:    
            sql_script = (""" DELETE FROM `ai per_loan_ldr` WHERE aiBankDate = date('%s') """% yesterday.strftime("%Y-%m-%d") )
            print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
            print(sql_script)
            conn.execute(sql_script)
            print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))

    with engine.begin() as conn:

        metadata = db.MetaData()
        insert_table = db.Table('ai per_loan_ldr', metadata,
            db.Column('aiBankDate', db.Date()),
            db.Column('loanId', db.String(20)),
            db.Column('certNo', db.String(20)),
            db.Column('custName', db.Text()),
            db.Column('totalTerms', db.BigInteger()),
            db.Column('totalDays', db.BigInteger()),
            db.Column('startDate', db.Date()),
            db.Column('endDate', db.Date()),
            db.Column('balance', db.Float()),
            db.Column('encashAmt', db.Float()),
            db.Column('rate', db.Float()),
            db.Column('dueDays', db.BigInteger()),
            db.Column('phonenumber', db.Text()),
            db.Column('idCardSex', db.Text()),
            db.Column('education', db.Text()),
            db.Column('industry', db.Text()),
            db.Column('industryExperience', db.Text()),
            db.Column('marry', db.Text()),
            db.Column('inHomeAddress', db.Text()),
            db.Column('companyAddress', db.Text()),
            db.Column('lastRepayDay', db.Date()),
            db.Column('nextRepayDay', db.Date()),
            db.Column('app_source', db.String(100)),
            db.Column('product', db.String(100)),
            db.Column('buyback_flag_today', db.BigInteger()),
            db.Column('buyback_flag_month', db.BigInteger()),
            db.Column('buyback_amt_today', db.Float()),
            db.Column('buyback_amt_month', db.Float()),
            db.Column('rccr', db.Integer()),
            db.Column('CreditTransid', db.String(35)),
            db.Column('ovdPrinBal', db.Float()),
            db.Column('ovdIntBal', db.Float()),
            db.Column('pnltIntBal', db.Float()),
            db.Column('idCardBirthday', db.Date()),
            db.Column('verified_income', db.Float()),
            db.Column('installment_amount', db.Float()),
            db.Column('outstanding', db.Float()),
            db.Column('age', db.Integer()),
            db.Column('birth_province', db.Text()),
            db.Column('birth_city', db.Text()),
            db.Column('birth_city_tier', db.Text()),
            db.Column('birth_district', db.Text()),
            db.Column('living_district', db.Text()),
            db.Column('living_province', db.Text()),
            db.Column('living_city_tier', db.Text()),
            db.Column('company_district', db.Text()),
            db.Column('company_province', db.Text()),
            db.Column('company_city', db.Text()),
            db.Column('living_city', db.Text()),
            db.Column('company_city_tier', db.Text())
             )


###########################################################################
##### TMP B buyback_amt_today
        sql_script=(""" DROP TEMPORARY TABLE IF EXISTS tmp_B """)
        print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
        print(sql_script)
        conn.execute(sql_script)
        sql_script=(""" 
            CREATE TEMPORARY TABLE tmp_B 
            (
                    `loan_id` varchar(30)           NOT     NULL,
                    `buyback_flag_today`    int     DEFAULT NULL,
                    `buyback_amt_today`     double  DEFAULT NULL,
            PRIMARY KEY tmp_B_key (`loan_id`)
            )
            ENGINE=InnoDB 
            SELECT loan_id,
                    1 as buyback_flag_today,
                    coalesce(cast(`buyback_total` as DECIMAL(13,2)),0) as buyback_amt_today
            FROM   `ai_buyback_report` 
            WHERE tran_date = '%s'
            """% yesterday.strftime("%Y-%m-%d") )
        print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
        print(sql_script)
        conn.execute(sql_script)
###########################################################################
##### TMP C buyback_amt_month
        sql_script=(""" DROP TEMPORARY TABLE IF EXISTS tmp_C """)
        print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
        print(sql_script)
        conn.execute(sql_script)
        sql_script=(""" 
            CREATE TEMPORARY TABLE tmp_C 
            (
                    `loan_id` varchar(30)           NOT     NULL,
                    `buyback_flag_month`    int     DEFAULT NULL,
                    `buyback_amt_month`     double  DEFAULT NULL,
            PRIMARY KEY tmp_C_key (`loan_id`)
            )
            ENGINE=InnoDB 
            SELECT loan_id,
                    1 as buyback_flag_month,
                    sum(coalesce(cast(`buyback_total` as DECIMAL(13,2)),0)) as buyback_amt_month
            FROM   `ai_buyback_report` 
            WHERE date(tran_date) >= date('%s') and date(tran_date) <= date('%s')
            GROUP BY loan_id
            """% (BEG_OF_MONTH.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")) )
        print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
        print(sql_script)
        conn.execute(sql_script)
###########################################################################
##### TMP D rccr
        sql_script=(""" DROP TEMPORARY TABLE IF EXISTS tmp_D """)
        print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
        print(sql_script)
        conn.execute(sql_script)
        sql_script=(""" 
            CREATE TEMPORARY TABLE tmp_D 
            (
                    `union_transaction_id`  varchar(128)    NOT     NULL,
                    `rccr`                  varchar(18)     DEFAULT NULL,
            PRIMARY KEY tmp_D_key (`union_transaction_id`)
            )
            ENGINE=InnoDB 
            SELECT  union_transaction_id, 
                    max(rccr) as rccr
            FROM `ai_credit_scoring_report`
            group by union_transaction_id
            """ )
        print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
        print(sql_script)
        conn.execute(sql_script)
###########################################################################
##### TMP E verified_income
        sql_script=(""" DROP TEMPORARY TABLE IF EXISTS tmp_E """)
        print(sql_script)
        print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
        conn.execute(sql_script)
        sql_script=(""" 
            CREATE TEMPORARY TABLE tmp_E 
            (
                    `union_transaction_id`  varchar(128)    NOT     NULL,
                    `verified_income`       double          DEFAULT NULL,
            PRIMARY KEY tmp_E_key (`union_transaction_id`)
            )
            ENGINE=InnoDB 
            SELECT  union_transaction_id,
                    min(verified_income) as verified_income
            FROM `ai_credit_limit_related_report`
            group by union_transaction_id
            """ )
        print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
        print(sql_script)
        conn.execute(sql_script)
###########################################################################
##### TMP F idCardBirthday
        sql_script=(""" DROP TEMPORARY TABLE IF EXISTS tmp_F """)
        print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
        print(sql_script)
        conn.execute(sql_script)
        sql_script=(""" 
            CREATE TEMPORARY TABLE tmp_F 
            (
                    `txn_srl_no`            varchar(128)    NOT     NULL,
                    `idCardBirthday`        varchar(40)     DEFAULT NULL,
            PRIMARY KEY tmp_F_key (`txn_srl_no`)
            )
            ENGINE=InnoDB 
            SELECT  txn_srl_no,
                    max(idCardBirthday) as idCardBirthday
            FROM `ai_credit_approval_report`
            group by txn_srl_no
            """ )
        print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
        print(sql_script)
        conn.execute(sql_script)
###########################################################################
##### SELECT TO TEMP
        print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
        list_file=glob.glob("""%sdata dict and std format *.xlsx""" % pth)
        list_file.sort()
        print("Load file: %s" % list_file[-1])
        adr=pd.read_excel(list_file[-1],sheet_name="address_mapping", dtype=str)
        prov=set(adr[(adr['category']=='province') & (adr['category'].notna())]['province'].tolist())
        city=set(adr[adr['city'].notna()]['city'].tolist())
        city_tier=adr[adr['city'].notna()][['city_tier','city']].drop_duplicates()
        district=set(adr[adr['district'].notna()]['district'].tolist())
        adr2=adr.copy().drop(columns=['no','category','data_year','province_realname','city_realname'])
        adr2.rename(columns={'province':'birth_province', 'city':'birth_city', 'city_tier':'birth_city_tier', 'district':'birth_district'}, inplace = True)
        
        sql_script=("""    
                                        SELECT  cast(A.aiBankDate  as date) as aiBankDate,
                                                A.loanId,
                                                A.certNo,
                                                A.custName,
                                                cast(A.totalTerms as UNSIGNED) as totalTerms,
                                                cast(A.totalDays as UNSIGNED) as totalDays,
                                                date(A.startDate) as startDate,
                                                date(A.endDate) as endDate,
                                                cast(A.balance as DECIMAL(13,2)) as balance,
                                                cast(A.encashAmt as DECIMAL(13,2)) as encashAmt,
                                                cast(A.rate as DECIMAL(13,4)) as rate,
                                                cast(A.dueDays as UNSIGNED) as dueDays,
                                                A.phonenumber,
                                                A.idCardSex,
                                                A.education,
                                                A.industry,
                                                A.industryExperience,
                                                A.marry,
                                                A.inHomeAddress,
                                                A.companyAddress,
                                                cast(A.lastRepayDay as date) as lastRepayDay,
                                                cast(A.nextRepayDay as date) as nextRepayDay,
                                                'ai' as app_source,
                                                'per_loan' as product,
                                                coalesce(B.buyback_flag_today,0) as buyback_flag_today,
                                                coalesce(C.buyback_flag_month,0) as buyback_flag_month,
                                                B.buyback_amt_today as buyback_amt_today,
                                                C.buyback_amt_month as buyback_amt_month,
                                                D.rccr,
                                                A.CreditTransid,
                                                cast(A.ovdPrinBal as DECIMAL(13,2)) as ovdPrinBal,
                                                cast(A.ovdIntBal as DECIMAL(13,2)) as ovdIntBal,
                                                cast(A.pnltIntBal as DECIMAL(13,2)) as pnltIntBal,
                                                cast(F.idCardBirthday as Date) as idCardBirthday,
                                                E.verified_income
                                        FROM        `ai_loan_daily_report` as A
                                        LEFT JOIN   `tmp_B` as B 
                                        ON      ( A.loanId = B.loan_id )
                                        LEFT JOIN   `tmp_C` as C 
                                        ON      ( A.loanId = C.loan_id )
                                        LEFT JOIN   `tmp_D` as D 
                                        ON      ( A.CreditTransid = D.union_transaction_id  )
                                        LEFT JOIN   `tmp_E` as E 
                                        ON      ( A.CreditTransid = E.union_transaction_id  )
                                        LEFT JOIN   `tmp_F` as F
                                        ON      ( A.CreditTransid = F.txn_srl_no  )
                                        WHERE A.aiBankDate = '%s' 
                                """% (
                                    yesterday.strftime("%Y-%m-%d")))
        print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
        print(sql_script)
        curr = conn.execute(sql_script)
        loopNumber = 0
        countNumber = 0
        print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))

        while True:
            rows = curr.fetchmany(100000)
            if not rows:
                break
            loopNumber = loopNumber + 1
            ldr = DataFrame(rows)
            countNumber = countNumber + len(ldr.index)
            print("** loop no: '%s' * %s"% (loopNumber, countNumber ))
            print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
            ldr.columns = curr.keys()

            def intstallment(encashAmt, rate, totalTerms):
                try:
                    dis_fac = 1 / (1 + rate/12)
                    installmentAmount = encashAmt * (1 - dis_fac) / (1 - dis_fac ** totalTerms)
                except:
                    installmentAmount=np.nan
                return installmentAmount
            
            ldr['installment_amount']=ldr.apply(lambda row: intstallment(row.encashAmt, row.rate, row.totalTerms), axis = 1)
            ldr['outstanding']=ldr.loc[:,['balance','pnltIntBal','ovdIntBal']].sum(axis=1)
            try:
                ldr['age']=date.today().year - datetime.strptime(ldr["idCardBirthday"], '%Y-%m-%d').date().year
            except:
                ldr['age']= np.nan

            #ldr=pd.DataFrame({"certNo":['123456789123456789','987654321123456789','110000','110000','110000'], 'birth_district':[np.NaN,np.NaN,np.NaN,np.NaN,np.NaN],"inHomeAddress":['北京市北京市通州区','','河北省唐山市丰润区','河北省邢台市宁晋县','河北省邢台市宁晋县']})
            #Mapping by id_no
            ldr['id_code']=ldr["certNo"].apply(lambda x: x.strip()[:6] if x!=None else x)
            ldr=ldr.drop(columns=['birth_province','birth_city','birth_city_tier','birth_district'], axis=1, errors='ignore')#if column available then drop it before going to create new one after joining table adr2
            ldr=ldr.set_index('id_code').join(adr2.set_index('id_code'), how='left', on='id_code')
            ldr.reset_index(drop=True, inplace=True)
            #Mapping by word 
            ldr['living_district']  =ldr["inHomeAddress"].apply(lambda x: '' if x in ['',None] else ( 'other' if list(filter(lambda y: x.count(y)>0, district))==[] else ' '.join(list(filter(lambda y: x.count(y)>0, district)))))
            ldr['living_province']  =ldr["inHomeAddress"].apply(lambda x: '' if x in ['',None] else ( 'other' if list(filter(lambda y: x.count(y)>0, prov))==[]     else ' '.join(list(filter(lambda y: x.count(y)>0, prov)))))
            ldr['living_city']      =ldr["inHomeAddress"].apply(lambda x: '' if x in ['',None] else ( 'other' if list(filter(lambda y: x.count(y)>0, city))==[]     else ' '.join(list(filter(lambda y: x.count(y)>0, city)))))
            ldr=ldr.set_index('living_city').join(city_tier.copy().rename(columns={'city':'living_city', 'city_tier':'living_city_tier'}).set_index('living_city'), how='left', on='living_city').reset_index(drop=False)
            
            ldr['company_district']  =ldr["companyAddress"].apply(lambda x: '' if x in ['',None] else ( 'other' if list(filter(lambda y: x.count(y)>0, district))==[] else ' '.join(list(filter(lambda y: x.count(y)>0, district)))))
            ldr['company_province']  =ldr["companyAddress"].apply(lambda x: '' if x in ['',None] else ( 'other' if list(filter(lambda y: x.count(y)>0, prov))==[]     else ' '.join(list(filter(lambda y: x.count(y)>0, prov)))))
            ldr['company_city']      =ldr["companyAddress"].apply(lambda x: '' if x in ['',None] else ( 'other' if list(filter(lambda y: x.count(y)>0, city))==[]     else ' '.join(list(filter(lambda y: x.count(y)>0, city)))))
            ldr=ldr.set_index('company_city').join(city_tier.copy().rename(columns={'city':'company_city', 'city_tier':'company_city_tier'}).set_index('company_city'), how='left', on='company_city').reset_index(drop=False)
            print("Time:", datetime.datetime.now().strftime("%H:%M:%S"), "before to_dict" )
            ldr = ldr.replace({np.nan: None})
            insert_dict = ldr.to_dict(orient="records")
            print("Time:", datetime.datetime.now().strftime("%H:%M:%S"), "after to_dict" )
            print("Time:", datetime.datetime.now().strftime("%H:%M:%S"), "before insert" )
            #ldr.to_sql(name='ai per_loan_ldr', con=conn, chunksize=10000, if_exists='append', index=False, method='multi')#'replace'
            with engine.begin() as conn2:
                batch_size=30000
                for batch_start in range(0, len(insert_dict), batch_size):
                    print("Time: {} Inserting {}-{}".format(datetime.datetime.now().strftime("%H:%M:%S"), batch_start, batch_start + batch_size))
                    conn2.execute(insert_table.insert(), insert_dict[batch_start:batch_start + batch_size])
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"), "after insert" )
            try:
                del ldr
                del insert_dict
            except:
                print("Error del obj")
            if ("Linux" == platform.system()):
                libc.malloc_trim(0)
            gc.collect()
            
###########################################################################
##### TEMP TO ACTUAL
    print("table:ai per_loan_ldr")
    print("================================")
    print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
    print("A02_3_0_roll_up_aibank.py: Completed")
    print("================================")
    running_flag = False
    thr1.join()
    sys.exit(0)

except Exception as error:
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
    strLog = "Failed to process A02_3_0_roll_up_aibank.py {}".format(error)
    print(strLog)
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    running_flag = False
    thr1.join()
    sys.exit(1)

###########################################################################
#####3_0_roll_up_aibank (merge ldr to buyback))##END###
###########################################################################
