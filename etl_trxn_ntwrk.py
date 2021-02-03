
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
    'owner': 'Aderinsola Oguntade',
    'start_date': days_ago(2)
}

dag = DAG(
    dag_id='air_etl_trxn_ntwrk',
    default_args=args,
    schedule_interval="@weekly",
    tags=['PEP'],
)

etl_args = {
    "dest_table_name" : "air_etl_trxn_ntwrk",
    "dest_table_columns_definition" : {
        "SENDER_BRA_CODE":("Number(4)", int), 
        "SENDER_CUS_NUM":("Number(7)", int), 
        "BENEF_BRA_CODE":("Number(4)", int), 
        "BENEF_CUS_NUM":("Number(7)", int),
        "TRA_AMT":("Number", float),
        "EQU_TRA_AMT":("Number", float),
        "TRA_COUNT":("Number", int),
        }, # highlights destination table fields excluding reference_dates and control_dates; simply agg_columns and agg_measures
    "source_query" : """
        select a.tra_date, a.bra_code sender_bra_code, a.cus_num sender_cus_num,
        b.bra_code benef_bra_code, b.cus_num benef_cus_num, a.tra_amt, a.equ_tra_amt
        from
        (select * from stg.src_transact
        where cus_num>99999
        and cur_code=1
        and expl_code=102
        and deb_cre_ind=1
        and can_rea_code=0) a  

        inner join
        (select * from stg.src_transact
        where cus_num>99999
        and cur_code=1
        and expl_code=102
        and deb_cre_ind=2) b

        on a.origt_tra_seq1=b.origt_tra_seq1 and a.origt_bra_code=b.origt_bra_code
        and a.tra_amt=b.tra_amt and a.tra_date=b.tra_date
        and not (a.bra_code=b.bra_code and a.cus_num=b.cus_num)
    """,
    "etl_period" : EtlPeriod.MONTHLY,
    "etl_period_freq" : 12,
    "source_control_date_field_name" : "TRA_DATE",
    "agg_columns" : ["SENDER_BRA_CODE", "SENDER_CUS_NUM", "BENEF_BRA_CODE", "BENEF_CUS_NUM"], # optional
    "agg_measures" : [
    ("count(1)", "TRA_COUNT"), 
    ("sum(tra_amt)", "TRA_AMT"), 
    ("sum(equ_tra_amt)", "EQU_TRA_AMT")],
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

