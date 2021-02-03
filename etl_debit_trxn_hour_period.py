
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
    dag_id='air_etl_debit_trxn_hour_period',
    default_args=args,
    schedule_interval=None,
    tags=['debit','transactions'],
)

etl_args = {
    "dest_table_name" : "air_etl_debit_trxn_hour_period",
    "dest_table_columns_definition" : {
        "BRA_CODE":("Number(4)", int), 
        "CUS_NUM":("Number(7)", int), 
        "tra_amt_0_5":("Number",float),
        "tra_amt_6_11":("Number",float),
        "tra_amt_12_16":("Number",float),
        "tra_amt_17_20":("Number",float),
        "tra_amt_21_23":("Number",float),
        "tra_count_0_5":("Number",float),
        "tra_count_6_11":("Number",float),
        "tra_count_12_16":("Number",float),
        "tra_count_17_20":("Number",float),
        "tra_count_21_23":("Number",float)}, # highlights destination table fields excluding reference_dates and control_dates; simply agg_columns and agg_measures
    "source_query" : """
        select bra_code, cus_num, tra_amt, tra_hour, tra_date  
        from
        (select a.*, substr(upd_time,1,2) tra_hour from stg.src_TRANSACT a
        where
        a.can_rea_code = 0
        and a.cus_num >= 100000
        and a.cur_code = 1
        and a.deb_cre_ind = 1 
        and expl_code not in (44,58,84,95,100,103,139,147,148,156,157,205,330,336,340,471,541,
        645,903,917,964,965,225,644,993)
        and a.led_code in (0059,0064,0065,0067,0082,5021,0024,0077,0002,0006,0008,0001,0026,0004,0012,
        0013,0016,0066,0068,0069,5020,5098,0057,0073,5023,0055,5022,5027,5024,5115,5025,5116,0023,5032))

    """,
    "etl_period" : EtlPeriod.MONTHLY,
    "etl_period_freq" : 12,
    "source_control_date_field_name" : "TRA_DATE",
    "agg_columns" : ["BRA_CODE","CUS_NUM"], # optional
    "agg_measures" : [
        ("nvl(sum(case when tra_hour between 0 and 5 then tra_amt end),0)", "tra_amt_0_5"),
        ("nvl(sum(case when tra_hour between 6 and 11 then tra_amt end),0)", "tra_amt_6_11"),
        ("nvl(sum(case when tra_hour between 12 and 16 then tra_amt end),0)", "tra_amt_12_16"),
        ("nvl(sum(case when tra_hour between 17 and 20 then tra_amt end),0)", "tra_amt_17_20"),
        ("nvl(sum(case when tra_hour between 21 and 23 then tra_amt end),0)", "tra_amt_21_23"),
        ("count(case when tra_hour between 0 and 5 then 1 end)", "tra_count_0_5"),
        ("count(case when tra_hour between 6 and 11 then 1 end)", "tra_count_6_11"),
        ("count(case when tra_hour between 12 and 16 then 1 end)", "tra_count_12_16"),
        ("count(case when tra_hour between 17 and 20 then 1 end)", "tra_count_17_20"),
        ("count(case when tra_hour between 21 and 23 then 1 end)", "tra_count_21_23")],
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

