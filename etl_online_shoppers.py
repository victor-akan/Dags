
from __future__ import print_function

import time
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import pickle
from airflow.hooks.base_hook import BaseHook
from multiprocessing import Process, Lock, Pool, cpu_count
import cx_Oracle
import pandas as pd
import calendar
from datetime import datetime, timedelta
from dateutil import relativedelta
import os
import shutil
import sys
sys.path.append("/root/Documents/Projects/CustomModules/")
from etlRunnerTemplate import *

args = {
    'owner': 'Data Analytics',
    'start_date': days_ago(2)
}

dag = DAG(
    dag_id='air_etl_online_shoppers',
    default_args=args,
    schedule_interval="@weekly",
    tags=['online','shoppers'],
)

etl_args = {
    "dest_table_name" : "air_etl_online_shoppers",
    "dest_table_columns_definition" : {
        "BRA_CODE":("Number(4)", int), 
        "CUS_NUM":("Number(7)", int), 
        "TRA_COUNT":("Number", int), 
        "TRA_AMT":("Number", float)}, # highlights destination table fields excluding reference_dates and control_dates; simply agg_columns and agg_measures
    "source_query" : """
        select bra_code, cus_num, tra_amt, tra_date from stg.src_transact
        where 
        (upper(remarks) like '%KONGA%' 
            or upper(remarks) like '%ECART%'
            or upper(remarks) like '%JUMIA%'
            or upper(remarks) like '%ALIBABA%'
            or upper(remarks) like '%PAYPORT%'
            or upper(remarks) like '%VCONNECT%'
            or upper(remarks) like '%AMAZON%'
            or upper(remarks) like '%PRINTIVO%'
            or upper(remarks) like '%AJEBO%MARKET%'
            or upper(remarks) like '%OBIWEZY%'
            or upper(remarks) like '%ALI%EXPRESS%'
            or upper(remarks) like '%PAY%PORT%'
            or upper(remarks) like '%KARA%'
            or upper(remarks) like '%YUDALA%'
            or upper(remarks) like '%5KSHOP%'
            or upper(remarks) like '%DEALDEY%'
            or upper(remarks) like '%OLX%'
            or upper(remarks) like '%JIJI%'
            or upper(remarks) like '%MOBOFREE%'
            or upper(remarks) like '%CHRISVICMALL%'   
            or upper(remarks) like '%PARKTEL%' 
            or upper(remarks) like '%KILIMALL%' 
            or upper(remarks) like '%ESCAPADE%' 
            or upper(remarks) like '%MYSTORE%' 
            or upper(remarks) like '%GIDIMALL%'
            or upper(remarks) like '%GLOO%'  
            or upper(remarks) like '%TECHMALL%'
            or upper(remarks) like '%HABARI%'
            or upper(remarks) like '%BUYRIGHT%'  
            or upper(remarks) like '%FOUANI%' 
            or upper(remarks) like '%GOODS%ARENA%'
            or upper(remarks) like '%AHIOMA%'  
            or upper(remarks) like '%OBEEZI%' 
            or upper(remarks) like '%AWUFU%' 
            or upper(remarks) like '%ESHOP%'  
            or upper(remarks) like '%MEGA%PLAZA%' 
            or upper(remarks) like '%SUPERMART%' 
            or upper(remarks) like '%BEST%BUY%FOR%LESS%' 
            or upper(remarks) like '%LUXATREL%' 
            or upper(remarks) like '%PARKWAY%NIGERIA%' 
            or upper(remarks) like '%ZABADNE%'     
   
        ) 
        and deb_cre_ind = 1
        and cus_num > 99999 and expl_code = 470
        and doc_alp = '9999' and can_rea_code = 0
    """,
    "etl_period" : EtlPeriod.MONTHLY,
    "etl_period_freq" : 12,
    "source_control_date_field_name" : "TRA_DATE",
    "agg_columns" : ["BRA_CODE","CUS_NUM"], # optional
    "agg_measures" : [
        ("COUNT(1)","TRA_COUNT"),
        ("NVL(SUM(TRA_AMT),0)", "TRA_AMT")],
    "optimize_extraction":True,
    "dest_reference_date_field_name" : None, #optional
    "dest_control_date_field_name" : None, #optional
    "etl_end_date" : None, #optional
    "staging_directory":None, #optional
}

dagWrapper = ETLDagWrapper(dag, etl_args)

dagWrapper.task_does_destination_table_exists >> [dagWrapper.task_create_destination_table, dagWrapper.task_filter_sourcing_sql_dates] >> dagWrapper.task_dress_sourcing_sql_queries
dagWrapper.task_dress_sourcing_sql_queries >> dagWrapper.task_does_staging_directory_exists
dagWrapper.task_does_staging_directory_exists >> [dagWrapper.task_clear_staging_directory, dagWrapper.task_create_staging_directory] >> dagWrapper.task_extract_starts
for i, t in enumerate(dagWrapper.etl_args["sourcing_sql_dates"]):
    d_from, d_to = t
    cur_operator = PythonOperator(
        task_id=f"extract_{i}",
        provide_context=True,
        python_callable=dagWrapper.extract,
        op_kwargs={"d_from":d_from, "d_to":d_to},
        dag=dag,
        # trigger_rule='none_failed_or_skipped'
    )
    dagWrapper.task_extract_starts >> cur_operator >> dagWrapper.task_extract_ends
dagWrapper.task_extract_ends >> dagWrapper.task_load >> dagWrapper.task_unique_test

