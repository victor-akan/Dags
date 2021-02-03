
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
    dag_id='air_etl_ind_accts_biz',
    default_args=args,
    schedule_interval="@weekly",
    tags=['individual accounts','business'],
)

etl_args = {
    "dest_table_name" : "air_etl_ind_accts_biz",
    "dest_table_columns_definition" : {
        "BRA_CODE":("Number(4)", int), 
        "CUS_NUM":("Number(7)", int), 
        "CUS_NAME":("Varchar(50)", object), 
        "CUS_EMAIL":("Varchar(50)", object), 
        "CUS_TELEPHONE":("Varchar(20)", object), 
        "BY_TRXN":("Number", int), 
        "BY_OBLIG_RATE":("Number", int), 
        "BY_POS":("Number", float)}, # highlights destination table fields excluding reference_dates and control_dates; simply agg_columns and agg_measures
    "source_query" : """
       SELECT nvl(nvl(aa.BRA_CODE,bb.BRA_CODE),cc.BRA_CODE) bra_code, nvl(nvl(aa.CUS_NUM,bb.CUS_NUM),cc.CUS_NUM) cus_num,
        nvl(nvl(upper(aa.cus_name),bb.cus_name),cc.cus_name)cus_name,
        nvl(nvl(aa.cus_telephone,bb.cus_telephone),cc.cus_telephone)cus_telephone,
        nvl(nvl(aa.cus_email,bb.cus_email),cc.cus_email)cus_email,
        nvl(by_trxn,0)by_trxn, nvl(by_oblig_rate,0)by_oblig_rate, nvl(by_POS,0)by_POS
        FROM (
        ind_biz_by_trxn aa
        full join ind_biz_by_oblig_rate bb
        on aa.bra_code = bb.bra_code and aa.cus_num = bb.cus_num
        full join ind_biz_by_pos cc
        on aa.bra_code = cc.bra_code and aa.cus_num = cc.cus_num)
    """,
    "etl_period" : EtlPeriod.MONTHLY,
    "etl_period_freq" : 12,
    "source_control_date_field_name" : "TRA_DATE",
    "agg_columns" : False, # optional
    "agg_measures" : False,
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

