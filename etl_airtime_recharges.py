
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
    'owner': 'EtlRunner',
    'start_date': days_ago(2)
}

dag = DAG(
    dag_id='air_etl_airtime_transact',
    default_args=args,
    schedule_interval="@weekly",
    tags=['airtime'],
)

etl_args = {
    "dest_table_name" : "air_etl_airtime_transact",
    "dest_table_columns_definition" : {
        "BRA_CODE":("Number(4)", int), 
        "CUS_NUM":("Number(7)", int), 
        "SELF_AIRTIME_COUNT":("Number", int), 
        "SELF_AIRTIME_VALUE":("Number", float), 
        "TOTAL_AIRTIME_COUNT":("Number", int), 
        "TOTAL_AIRTIME_VALUE":("Number", float)}, # highlights destination table fields excluding reference_dates and control_dates; simply agg_columns and agg_measures
    "source_query" : """select tra_date,a.bra_code,a.cus_num,tra_amt,crnt_bal,lpad(substr(remarks,-10),11,0) receiver_mob_num,
                        b.cus_mobile sender_cus_mobile,b.cus_telephone sender_cus_telephone
                        from stg.src_transact a, stg.src_customer_extd@exadata_new b
                        where a.bra_code=b.bra_code and a.cus_num=b.cus_num
                        and a.expl_code=32 and can_rea_code=0 and a.cus_num>99999 and deb_cre_ind=1""",
    "etl_period" : EtlPeriod.MONTHLY,
    "etl_period_freq" : 12,
    "source_control_date_field_name" : "TRA_DATE",
    "agg_columns" : ["BRA_CODE","CUS_NUM"], # optional
    "agg_measures" : [
        ("COUNT(CASE WHEN RECEIVER_MOB_NUM=SENDER_CUS_MOBILE OR RECEIVER_MOB_NUM=SENDER_CUS_TELEPHONE THEN 1 END)","SELF_AIRTIME_COUNT"),
        ("NVL(SUM(CASE WHEN RECEIVER_MOB_NUM=SENDER_CUS_MOBILE OR RECEIVER_MOB_NUM=SENDER_CUS_TELEPHONE THEN TRA_AMT END),0)", "SELF_AIRTIME_VALUE"),
        ("COUNT(1)", "TOTAL_AIRTIME_COUNT"),
        ("NVL(SUM(TRA_AMT),0)", "TOTAL_AIRTIME_VALUE")],
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
for d_from, d_to in dagWrapper.etl_args["sourcing_sql_dates"]:
    cur_operator = PythonOperator(
        task_id=f"extract_{d_from}_{d_to}",
        provide_context=True,
        python_callable=dagWrapper.extract,
        op_kwargs={"d_from":d_from, "d_to":d_to},
        dag=dag,
        # trigger_rule='none_failed_or_skipped'
    )
    dagWrapper.task_extract_starts >> cur_operator >> dagWrapper.task_extract_ends
dagWrapper.task_extract_ends >> dagWrapper.task_load >> dagWrapper.task_unique_test

