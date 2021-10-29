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
from multiprocessing import Pool
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
    #####3_1_factor_wcd aibank##START####
    ###########################################################################
    def extract_data_for_bscore(today, obs_period=6):
        if today.day == 1:
            creditscore = pickle.load(open("""%screditscore.pkl""" % (pth), 'rb'))
            list_date = [today + relativedelta(months=-m, day=31) for m in range(1,obs_period+1)]
            previous = today + relativedelta(months=-2, day=31)
 
            with engine.begin() as conn:
                sql_script=(""" DROP TEMPORARY TABLE IF EXISTS tmp_E02_t1""")
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                sql_script=(""" 
                    CREATE TEMPORARY TABLE tmp_E02_t1 
                    (
                        `app_source` varchar(100) DEFAULT NULL,
                        `product` varchar(100) DEFAULT NULL,
                        `pos_dt` date DEFAULT NULL,
                        `acc_no` varchar(20) NOT NULL,
                        `id_no` varchar(20) DEFAULT NULL,
                        `mob` bigint DEFAULT NULL,
                        `out_0m_b` double DEFAULT NULL,
                        `out_1m_b` double DEFAULT NULL,
                        `out_2m_b` double DEFAULT NULL,
                        `out_3m_b` double DEFAULT NULL,
                        `out_4m_b` double DEFAULT NULL,
                        `out_5m_b` double DEFAULT NULL,
                        `limit` double DEFAULT NULL,
                        `interest_rate` double DEFAULT NULL,
                        `installment_amount` double DEFAULT NULL,
                        `first_payment_default_flag` bigint DEFAULT NULL,
                        `first_three_payments_default_flag` bigint DEFAULT NULL,
                        `dlq_class` text,
                        `max_dpd_0m_b` bigint DEFAULT NULL,
                        `dpd` bigint DEFAULT NULL,
                        `totalTerms` bigint DEFAULT NULL,
                        `rccr` int DEFAULT NULL,
                        `verified_income` double DEFAULT NULL,
                        PRIMARY KEY tmp_E02_t1_key (`acc_no`)
                    )
                    ENGINE=InnoDB 
                    SELECT  ss.app_source, ss.product, ss.pos_dt, ss.acc_no, ss.id_no, ss.mob, 
                            ss.out_0m_b, ss.out_1m_b, ss.out_2m_b, ss.out_3m_b, ss.out_4m_b, ss.out_5m_b, 
                            ss.`limit`, ss.interest_rate, ss.installment_amount, ss.first_payment_default_flag, 
                            ss.first_three_payments_default_flag, ss.dlq_class,
                            ss.max_dpd_0m_b, ss.max_dpd_l5m_b, ss.dpd, r.totalTerms, r.rccr, r.verified_income
                    FROM `std_m_ldr_acc` as ss
                    LEFT JOIN `ai per_loan_ldr` as r
                    ON ( ss.acc_no = r.loanId and r.aiBankDate = date('%s') )
                    WHERE ss.pos_dt = date('%s') 
                    AND ss.app_source = 'ai'
                """% (list_date[0].strftime("%Y-%m-%d"), list_date[0].strftime("%Y-%m-%d")) )
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))

                sql_script=(""" DROP TEMPORARY TABLE IF EXISTS tmp_E02_t3""")
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                sql_script=(""" 
                    CREATE TEMPORARY TABLE tmp_E02_t3 
                    (
                    `acc_no` varchar(20) NOT NULL,
                    `consecu_payback` bigint DEFAULT NULL,
                    `consecu_ontime` bigint DEFAULT NULL,
                    PRIMARY KEY tmp_E02_t3_key (`acc_no`)
                    )
                    ENGINE=InnoDB 
                    SELECT acc_no, consecu_payback, consecu_ontime
                    FROM `ai per_loan_bscore_factor`
                    WHERE pos_dt = date('%s')
                """% previous.strftime("%Y-%m-%d") )
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))

                sql_script=(""" DROP TEMPORARY TABLE IF EXISTS tmp_E02_s""")
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                sql_script=(""" 
                    CREATE TEMPORARY TABLE tmp_E02_s
                    (
                        `acc_no` varchar(20) NOT NULL,
                        `pos_dt` date DEFAULT NULL,
                        `mob` bigint DEFAULT NULL,
                        PRIMARY KEY tmp_E02_s_key (`acc_no`)
                    )
                    ENGINE=InnoDB 
                    SELECT  acc_no, pos_dt, mob
                    FROM    `std_m_ldr_acc` 
                    WHERE   pos_dt IN (date('%s'), date('%s'), date('%s'), date('%s'), date('%s'), date('%s'))
                    AND     mob > 0
                    AND     app_source = 'ai'
                """% (list_date[0].strftime("%Y-%m-%d"), list_date[1].strftime("%Y-%m-%d"), list_date[2].strftime("%Y-%m-%d"), 
                   list_date[3].strftime("%Y-%m-%d"), list_date[4].strftime("%Y-%m-%d"), list_date[5].strftime("%Y-%m-%d")) )
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))

                sql_script=(""" DROP TEMPORARY TABLE IF EXISTS tmp_E02_m0""")
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                sql_script=(""" 
                    CREATE TEMPORARY TABLE tmp_E02_m0 
                    (
                    `acc_no` varchar(20) NOT NULL,
                    `pos_dt` date NOT NULL,
                    `buyback_flag_month` bigint DEFAULT NULL,
                     PRIMARY KEY tmp_E02_m0_key (`acc_no`,`pos_dt`)
                    )
                    ENGINE=InnoDB 
                    SELECT loanId as acc_no, aiBankDate as pos_dt, buyback_flag_month
                    FROM  `ai per_loan_ldr`
                    WHERE  aiBankDate = date('%s')
                """% list_date[0].strftime("%Y-%m-%d"))
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))

                sql_script=(""" DROP TEMPORARY TABLE IF EXISTS tmp_E02_m1""")
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                sql_script=(""" 
                    CREATE TEMPORARY TABLE tmp_E02_m1 
                    (
                    `acc_no` varchar(20) NOT NULL,
                    `pos_dt` date NOT NULL,
                    `buyback_flag_month` bigint DEFAULT NULL,
                     PRIMARY KEY tmp_E02_m1_key (`acc_no`,`pos_dt`)
                    )
                    ENGINE=InnoDB 
                    SELECT loanId as acc_no, aiBankDate as pos_dt, buyback_flag_month
                    FROM  `ai per_loan_ldr`
                    WHERE  aiBankDate = date('%s')
                """% list_date[1].strftime("%Y-%m-%d"))
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))

                sql_script=(""" DROP TEMPORARY TABLE IF EXISTS tmp_E02_m2""")
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                sql_script=(""" 
                    CREATE TEMPORARY TABLE tmp_E02_m2 
                    (
                    `acc_no` varchar(20) NOT NULL,
                    `pos_dt` date NOT NULL,
                    `buyback_flag_month` bigint DEFAULT NULL,
                     PRIMARY KEY tmp_E02_m2_key (`acc_no`,`pos_dt`)
                    )
                    ENGINE=InnoDB 
                    SELECT loanId as acc_no, aiBankDate as pos_dt, buyback_flag_month
                    FROM  `ai per_loan_ldr`
                    WHERE  aiBankDate = date('%s')
                """% list_date[2].strftime("%Y-%m-%d"))
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))

                sql_script=(""" DROP TEMPORARY TABLE IF EXISTS tmp_E02_m3""")
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                sql_script=(""" 
                    CREATE TEMPORARY TABLE tmp_E02_m3
                    (
                    `acc_no` varchar(20) NOT NULL,
                    `pos_dt` date NOT NULL,
                    `buyback_flag_month` bigint DEFAULT NULL,
                     PRIMARY KEY tmp_E02_m3_key (`acc_no`,`pos_dt`)
                    )
                    ENGINE=InnoDB 
                    SELECT loanId as acc_no, aiBankDate as pos_dt, buyback_flag_month
                    FROM  `ai per_loan_ldr`
                    WHERE  aiBankDate = date('%s')
                """% list_date[3].strftime("%Y-%m-%d"))
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))

                sql_script=(""" DROP TEMPORARY TABLE IF EXISTS tmp_E02_m4""")
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                sql_script=(""" 
                    CREATE TEMPORARY TABLE tmp_E02_m4 
                    (
                    `acc_no` varchar(20) NOT NULL,
                    `pos_dt` date NOT NULL,
                    `buyback_flag_month` bigint DEFAULT NULL,
                     PRIMARY KEY tmp_E02_m4_key (`acc_no`,`pos_dt`)
                    )
                    ENGINE=InnoDB 
                    SELECT loanId as acc_no, aiBankDate as pos_dt, buyback_flag_month
                    FROM  `ai per_loan_ldr`
                    WHERE  aiBankDate = date('%s')
                """% list_date[4].strftime("%Y-%m-%d"))
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))

                sql_script=(""" DROP TEMPORARY TABLE IF EXISTS tmp_E02_m5""")
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                sql_script=(""" 
                    CREATE TEMPORARY TABLE tmp_E02_m5 
                    (
                    `acc_no` varchar(20) NOT NULL,
                    `pos_dt` date NOT NULL,
                    `buyback_flag_month` bigint DEFAULT NULL,
                     PRIMARY KEY tmp_E02_m5_key (`acc_no`,`pos_dt`)
                    )
                    ENGINE=InnoDB 
                    SELECT loanId as acc_no, aiBankDate as pos_dt, buyback_flag_month
                    FROM  `ai per_loan_ldr`
                    WHERE  aiBankDate = date('%s')
                """% list_date[5].strftime("%Y-%m-%d"))
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                conn.execute(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))

                sql_script = """
                    SELECT 
                        t1.app_source, t1.product, t1.pos_dt, t1.acc_no, t1.id_no, t1.mob, 
                        t1.out_0m_b, t1.out_1m_b, t1.out_2m_b, t1.out_3m_b, t1.out_4m_b, t1.out_5m_b, 
                        t1.limit, t1.interest_rate, t1.installment_amount, t1.first_payment_default_flag, 
                        t1.first_three_payments_default_flag, t1.dlq_class,
                        t1.max_dpd_0m_b, t1.totalTerms, t1.max_dpd_l5m_b, t1.dpd, 
                        IFNULL(t1.rccr, 12) as rccr_a_sc,
                        IFNULL(t1.verified_income, 0) as verified_income,
                        IFNULL(t2.payback_pattern,'xxxxxx') as payback_pattern,
                        IFNULL(t2.buyback_flag_month,0) as buyback_flag_month,
                        IFNULL(t3.consecu_payback, 0) as consecu_payback, 
                        IFNULL(t3.consecu_ontime, 0) as consecu_ontime
                    FROM tmp_E02_t1 as t1
                    LEFT JOIN 
                    (
                        SELECT s.acc_no, s.pos_dt, s.mob, IFNULL(m0.buyback_flag_month,0) buyback_flag_month,
                        CONCAT(
                            IFNULL(m5.buyback_flag_month, 'x'),
                            IFNULL(m4.buyback_flag_month, 'x'),
                            IFNULL(m3.buyback_flag_month, 'x'),
                            IFNULL(m2.buyback_flag_month, 'x'),
                            IFNULL(m1.buyback_flag_month, 'x'),
                            IFNULL(m0.buyback_flag_month, 'x')
                            ) as payback_pattern
                        FROM tmp_E02_s as s 
                        LEFT JOIN tmp_E02_m0 as m0 
                        ON (s.acc_no = m0.acc_no and s.pos_dt = m0.pos_dt )
                        LEFT JOIN tmp_E02_m1 as m1 
                        ON (s.acc_no = m1.acc_no and s.pos_dt = m1.pos_dt )
                        LEFT JOIN tmp_E02_m2 as m2 
                        ON (s.acc_no = m2.acc_no and s.pos_dt = m2.pos_dt )
                        LEFT JOIN tmp_E02_m3 as m3 
                        ON (s.acc_no = m3.acc_no and s.pos_dt = m3.pos_dt )
                        LEFT JOIN tmp_E02_m4 as m4 
                        ON (s.acc_no = m4.acc_no and s.pos_dt = m4.pos_dt )
                        LEFT JOIN tmp_E02_m5 as m5 
                        ON (s.acc_no = m5.acc_no and s.pos_dt = m5.pos_dt )
                    ) t2 ON t1.acc_no = t2.acc_no
                    LEFT JOIN tmp_E02_t3 as t3
                    ON t1.acc_no = t3.acc_no
                    """
                print(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                curr =conn.execute(sql_script)
                loopNumber = 0
                countNumber = 0
                while True:
                    rows = curr.fetchmany(30000)
                    if not rows:
                        break
                    loopNumber = loopNumber + 1
                    ldr_all = DataFrame(rows)
                    countNumber = countNumber + len(ldr_all.index)
                    print("** loop no: '%s' * %s"% (loopNumber, countNumber ))
                    print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                    ldr_all.columns = curr.keys()
                    
                    ldr_all['count'] = ldr_all['payback_pattern'].apply(lambda x: Counter(x))
                    ldr_all['count_ontime_l6m'] = ldr_all['count'].apply(lambda x: x.get('0', 0))
                    ldr_all['count_payback_l6m'] = ldr_all['count'].apply(lambda x: x.get('1', 0))
                    ldr_all['consec'] = ldr_all['payback_pattern'].apply(lambda x:{label: sum(1 for _ in group) for label, group in groupby(x)})
                    ldr_all['max_con_ontime_l6m'] = ldr_all['consec'].apply(lambda x: x.get('0', 0))
                    ldr_all['max_con_payback_l6m'] = ldr_all['consec'].apply(lambda x: x.get('1', 0))
                    

                    #print("before behavioral_factors: ", datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
                    #json_temp1 = behavioral_factors(ldr_all, today)
                    #print("before behavioral_factors: ", datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
                    ldr_all['consecu_payback'] = ldr_all.apply(lambda row: row.consecu_payback + 1 if row.buyback_flag_month == 1 else 0, axis=1)
                    ldr_all['consecu_ontime'] = ldr_all.apply(lambda row: row.consecu_ontime + 1 if row.dpd == 0 else 0, axis=1)
                    ##df1 = pd.merge(df_last, balance, on=['acc_no'], how='left')
                    ldr_all['abc'] = ldr_all['installment_amount']
                    ldr_all['current_debt_burden'] = ldr_all.apply(lambda row: row.abc / row.verified_income if row.verified_income > 0 else np.nan, axis=1)

                    ldr_all.rename(columns={
                                'abc':'monthly_repayment_(PBOC)'
                            }, inplace=True)

                    ldr_all['mob_to_term'] = ldr_all['mob'] / ldr_all['totalTerms']

                    ldr_all['balance_end_0m_b'] = ldr_all.apply(lambda row: out_end(row.limit, row.interest_rate, row.installment_amount, row.mob-0), axis = 1) 
                    ldr_all['balance_end_1m_b'] = ldr_all.apply(lambda row: out_end(row.limit, row.interest_rate, row.installment_amount, row.mob-1), axis = 1) 
                    ldr_all['balance_end_2m_b'] = ldr_all.apply(lambda row: out_end(row.limit, row.interest_rate, row.installment_amount, row.mob-2), axis = 1) 
                    ldr_all['balance_end_3m_b'] = ldr_all.apply(lambda row: out_end(row.limit, row.interest_rate, row.installment_amount, row.mob-3), axis = 1) 
                    ldr_all['balance_end_4m_b'] = ldr_all.apply(lambda row: out_end(row.limit, row.interest_rate, row.installment_amount, row.mob-4), axis = 1) 
                    ldr_all['balance_end_5m_b'] = ldr_all.apply(lambda row: out_end(row.limit, row.interest_rate, row.installment_amount, row.mob-5), axis = 1) 
                    ldr_all['act_to_sched_out_0m_b'] = ldr_all.apply(lambda row: act_to_sch(row.out_0m_b, row.balance_end_0m_b), axis = 1)
                    ldr_all['act_to_sched_out_1m_b'] = ldr_all.apply(lambda row: act_to_sch(row.out_1m_b, row.balance_end_1m_b), axis = 1)
                    ldr_all['act_to_sched_out_2m_b'] = ldr_all.apply(lambda row: act_to_sch(row.out_2m_b, row.balance_end_2m_b), axis = 1)
                    ldr_all['act_to_sched_out_3m_b'] = ldr_all.apply(lambda row: act_to_sch(row.out_3m_b, row.balance_end_3m_b), axis = 1)
                    ldr_all['act_to_sched_out_4m_b'] = ldr_all.apply(lambda row: act_to_sch(row.out_4m_b, row.balance_end_4m_b), axis = 1)
                    ldr_all['act_to_sched_out_5m_b'] = ldr_all.apply(lambda row: act_to_sch(row.out_5m_b, row.balance_end_5m_b), axis = 1)
                    ldr_all['group_0m_b'] = ldr_all['act_to_sched_out_0m_b'].apply(group_act_to_sch)
                    ldr_all['group_1m_b'] = ldr_all['act_to_sched_out_1m_b'].apply(group_act_to_sch)
                    ldr_all['group_2m_b'] = ldr_all['act_to_sched_out_2m_b'].apply(group_act_to_sch)
                    ldr_all['group_3m_b'] = ldr_all['act_to_sched_out_3m_b'].apply(group_act_to_sch)
                    ldr_all['group_4m_b'] = ldr_all['act_to_sched_out_4m_b'].apply(group_act_to_sch)
                    ldr_all['group_5m_b'] = ldr_all['act_to_sched_out_5m_b'].apply(group_act_to_sch)
                    ldr_all['pattern_ratio'] = (ldr_all['group_5m_b'].astype(str) + '-' + ldr_all['group_4m_b'].astype(str) + '-' +
                                                ldr_all['group_3m_b'].astype(str) + '-' + ldr_all['group_2m_b'].astype(str) + '-' +
                                                ldr_all['group_1m_b'].astype(str) + '-' + ldr_all['group_0m_b'].astype(str))
                    ldr_all.drop(['balance_end_0m_b','balance_end_1m_b','balance_end_2m_b','balance_end_3m_b','balance_end_4m_b','balance_end_5m_b','group_0m_b', 'group_1m_b', 'group_2m_b','group_3m_b','group_4m_b','group_5m_b'], axis=1)
                    
                    print("before creditscore: ", datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
                    json_temp1 = creditscore(ldr_all)
                    print("before creditscore: ", datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
                    
                    print("before grad: ", datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
                    b_sc_acc = grad(json_temp1["json_temp"])
                    print("after grad: ", datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))

                    table_name = json_temp1['table_name']
                    b_sc_acc.to_sql(name='b_sc_acc', con=conn, chunksize=10000, if_exists='append', index=False, method='multi')#'replace'
                    
                    json_temp1["score"] = json_temp1["score"].drop(columns=['count', 'consec'])
                    json_temp1["score"].to_sql(name=table_name, con=conn, chunksize=10000, if_exists='append', index=False, method='multi')#'replace'

                    ldr_all = ldr_all.drop(columns=['count', 'consec'])
                    ldr_all.to_sql(name='ai per_loan_bscore_factor', con=conn, chunksize=10000, if_exists='append', index=False, method='multi')#'replace'
                    try:
                        del ldr_all
                    except:
                        print("Error del obj")
                    if ("Linux" == platform.system()):
                        libc.malloc_trim(0)
                    gc.collect()

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
    
    def act_to_sched(df, today):
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
        
        df_last = df_last[['acc_no', 'group_0m_b', 'group_1m_b', 'group_2m_b', 'group_3m_b', 'group_4m_b', 'group_5m_b', 'pattern_ratio','act_to_sched_out_0m_b','act_to_sched_out_1m_b','act_to_sched_out_2m_b','act_to_sched_out_3m_b','act_to_sched_out_4m_b', 'act_to_sched_out_5m_b']]
        
        return df_last
    
    def behavioral_factors(df, today):
        balance = act_to_sched(df, today)
        df_last = df.copy()
        try:
            #previous = today + relativedelta(months=-2, day=31)
            #sql_script = ("""SELECT acc_no, consecu_payback, consecu_ontime
            #                 FROM `ai per_loan_bscore_factor`
            #                 WHERE DATE(pos_dt) = date('%s') """ % previous.strftime("%Y-%m-%d"))
            #print("before: ", datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
            #print(sql_script)
            #curr = engine.execute(sql_script)
            #f = DataFrame(curr.fetchall())
            #f.columns = ['acc_no', 'consecu_payback', 'consecu_ontime']
    
            #df_last = pd.merge(df_last, f, on=['acc_no'], how='left')
            df_last['consecu_payback'] = df_last.apply(lambda row: row.consecu_payback + 1 if row.buyback_flag_month == 1 else 0, axis=1)
            df_last['consecu_ontime'] = df_last.apply(lambda row: row.consecu_ontime + 1 if row.dueDays == 0 else 0, axis=1)
                
        except error as e:
            print("error behavioral_factors %s" % e)
            df_last['consecu_payback'] = df_last['buyback_flag_month'].apply(lambda x: 1 if x == 1 else 0)
            df_last['consecu_ontime'] = df_last['max_dpd_0m_b'].apply(lambda x: 1 if x == 0 else 0) * df_last['mob']
            
        ##df1 = pd.merge(df_last, balance, on=['acc_no'], how='left')
        df_last['monthly_repayment_(PBOC)'] = df_last['installment_amount']
        df_last['current_debt_burden'] = np.nan #df3['monthly_repayment_(PBOC)'] / df3['verified_income']
        df_last['mob_to_term'] = df_last['mob'] / df_last['totalTerms']
        #df_last['app_source'] = 'ai'
        #df_last['product'] = 'per_loan'
        

        df_last = df_last.copy()
        df_last.rename(columns={
            'rccr':'rccr_a_sc'
        }, inplace=True)
        df_last.columns = [x.lower() for x in df_last.columns]

        return df_last
    ###########################################################################
    #####3_1_factor_aibank##END####
    ###########################################################################
    
    ##############################################################################################
    ####3_1_1_ b_score###START######
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
        #card=pickle.load(open(list_file[-1], 'rb'))
        card={k.lower(): v for k, v in card.items()}#Lower case dictionary key
        for i in card.keys():
            card[i]=card[i].applymap(lambda s:s.lower() if isinstance(s, str) else s)
        json_temp2=json_temp[(json_temp['app_source']==app_source) & (json_temp['product']==product)]

        #print("json_temp2 headers")
        #for col in json_temp2.columns:
        #    print(col)
        #print("card headers")
        #print(card)
        #for col in card.columns:
        #    print(col)
        score = sc.scorecard_ply(json_temp2, card, only_total_score=False)
        score = pd.concat([json_temp, score], axis=1, sort=False)
        score.rename(columns={'score':'b_sc'}, inplace=True)
        score["score_file"]=os.path.basename(list_file[-1])
        #score[["app_source","product", "score_file"]] = pd.DataFrame([[app_source, product, os.path.basename(list_file[-1])]], index=score.index)
        table_name = "%s" % (os.path.basename(list_file[-1])[5:-5])
        #score.to_sql(name=table_name, con=engine, if_exists='append', index=False)#'replace'      
        #try:
        json_temp=score[['pos_dt','app_source','product','id_no','acc_no','mob','rccr_a_sc','b_sc','dlq_class','dpd']]
        #except error as e:
        #    print("error son_temp %s" % e)
        #    json_temp=score[['pos_dt','app_source','product','id_no','acc_no','mob','b_sc','dlq_class','dpd']]
        #    json_temp['rccr_a_sc'] =np.NaN
         #result_array['json_temp'] = json_temp.to_json(orient="records")
        result_array = {}
        result_array['json_temp'] = json_temp
        result_array['score'] = score
        result_array['table_name'] = table_name
        return result_array
    # =============================================================================
    pickle.dump(creditscore, open('./credit review/creditscore.pkl', 'wb'))
    # =============================================================================
    
    ##############################################################################################
    ####3_1_1_ b_score###END######
    #####Monthly-acc_no####
    ##############################################################################################
    
    ##############################################################################################
    ####3_1_2_grad_pick_ab_rating###START######
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
            #cutoff=6
            cutoff= cut_off[(cut_off['app_source']==app_source) & (cut_off['product']==product)]['cut_off'].to_list()[0]  
            ldr['rccr_acc']=ldr[['rccr_a_sc', 'rccr_b_sc']].max(axis=1)
            ldr.loc[ldr['mob'] >= cutoff, 'rccr_acc'] = ldr['rccr_b_sc']
            ldr['rccr_acc']=ldr[['rccr_acc','dpd']].apply(lambda x: 13 if x['dpd']>90 else x['rccr_acc'], axis=1)
        except error as e:
            print("grad error %s" % e)
            ldr['rccr_a_sc']=np.NaN
            ldr['rccr_acc']=ldr['rccr_b_sc']
            ldr['rccr_acc']=ldr[['rccr_acc','dpd']].apply(lambda x: 13 if x['dpd']>90 else x['rccr_acc'], axis=1)
        b_sc_acc=ldr[['pos_dt', 'id_no', 'app_source', 'product', 'acc_no', 'mob', 'rccr_a_sc', 'rccr_b_sc', 'rccr_acc']]

        return b_sc_acc
    ##############################################################################################
    ####3_1_2_grad_pick_ab_rating###END######
    #####Monthly-acc_no####
    ##############################################################################################
    
    #######################################
    ######Run 3_1-3_1_2 aibank #######
    #######################################
    yesterday =tod + relativedelta(days=-3)
    eom       =yesterday + relativedelta(day=31)
    yeom      =yesterday + relativedelta(months=-1, day=31)
    today     =yesterday+ relativedelta(days=+1)

    b_sc_acc = {}
    json_temp1 = {}
    df4 = {}

    if (today.day == 1):
        if rerun_flag == "yes":
            with engine.begin() as conn:        
                sql_script = (""" DELETE FROM `b_sc_acc` WHERE pos_dt = date('%s') and app_source = 'ai' and product='per_loan' """% yesterday.strftime("%Y-%m-%d") )
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                print(sql_script)
                conn.execute(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                
                sql_script = (""" DELETE FROM `ai per_loan_bscore_factor` WHERE pos_dt = date('%s') """% yesterday.strftime("%Y-%m-%d") )
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                print(sql_script)
                conn.execute(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))

                sql_script = (""" DELETE FROM `ai per_loan b champ 20201125` WHERE pos_dt = date('%s') """% yesterday.strftime("%Y-%m-%d") )
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
                print(sql_script)
                conn.execute(sql_script)
                print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))

        print("before extract_data_for_bscore: ", datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
        extract_data_for_bscore(today)
        print("pass extract_data_for_bscore: ", datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
        print("table: b_sc_acc, `ai per_loan_bscore_factor`, `ai per_loan b champ 20201125`")

    print("================================")
    print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
    print("E02_3_1_factor_aibank.py: Completed")
    print("================================")
    running_flag = False
    thr1.join()
    sys.exit(0)
    
except Exception as error:
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("Time:", datetime.datetime.now().strftime("%H:%M:%S"))
    strLog = "Failed to process E02_3_1_factor_aibank.py {}".format(error)
    print(strLog)
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    running_flag = False
    thr1.join()
    sys.exit(1)
