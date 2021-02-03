
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
    dag_id='air_etl_quickcredit',
    default_args=args,
    schedule_interval=None,
    tags=['quickcredit'],
)

etl_args = {
    "dest_table_name" : "air_etl_quickcredit",
    "dest_table_columns_definition" : {
        "BRA_CODE":("Number(4)", int), 
        "CUS_NUM":("Number(7)", int), 
        "TRA_SEQ1":("Number", int), 
        "QC_TRA_SEQ2":("Number", int), 
        "CUR_CODE":("Number", int), 
        "LED_CODE":("Number", int), 
        "SUB_ACCT_CODE":("Number", int), 
        "QC_TRA_AMT":("Number", int), 
        "QC_DEB_CRE_IND":("Number", int), 
        "QC_CRNT_BAL":("Number", int), 
        "EXPL_CODE":("Number", int), 
        "SET_TRA_SEQ2":("Number", int), 
        "SET_LED_CODE":("Number", int), 
        "SET_SUB_ACCT_CODE":("Number", int), 
        "SET_DEB_CRE_IND":("Number", int), 
        "SET_CRNT_BAL":("Number", float)}, # highlights destination table fields excluding reference_dates and control_dates; simply agg_columns and agg_measures
    "source_query" : """
        select a.tra_seq1, a.tra_seq2 qc_tra_seq2, a.TRA_DATE, a.BRA_CODE, a.CUS_NUM, a.CUR_CODE, a.LED_CODE, a.SUB_ACCT_CODE,
        a.TRA_AMT qc_tra_amt, a.DEB_CRE_IND qc_deb_cre_ind, a.CRNT_BAL qc_crnt_bal, a.EXPL_CODE,
        b.tra_seq2 set_tra_seq2, b.LED_CODE set_led_code, b.SUB_ACCT_CODE set_sub_acct_code,
        b.DEB_CRE_IND set_deb_cre_ind, b.CRNT_BAL set_crnt_bal
        from
        (select * from stg.src_transact where
            cur_code=1 and led_code in (1118,1121) and cus_num>99999 and expl_code in (309,225,330,336)) a,
        (select * from stg.src_transact where 
            cur_code=1 and led_code in (1,2,4,6,8,12,13,16,23,26,59,65,67,82)) b
        where a.tra_date=b.tra_date
        and a.bra_code=b.bra_code
        and a.cus_num=b.cus_num
        and a.expl_code=b.expl_code
        and a.tra_amt=b.tra_amt
        and a.tra_seq1=b.tra_seq1
    """,
    "etl_period" : EtlPeriod.MONTHLY,
    "etl_period_freq" : 12,
    "source_control_date_field_name" : "TRA_DATE",
    "agg_columns" : None, # optional
    "agg_measures" : None,
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

