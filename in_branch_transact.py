
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
    dag_id='air_in_branch_transact',
    default_args=args,
    schedule_interval=None,
    tags=['in_branch'],
)


etl_args = {
    "dest_table_name" : "air_in_branch_transact",
    "dest_table_columns_definition" : {
        "BRA_CODE":("Number(4)", int), 
        "CUS_NUM":("Number(7)", int), 
        "ORIGT_BRA_CODE":("Number", int), 
        "DEB_CRE_IND":("Number", int), 
        "EXPL_CODE":("Number", int), 
        "TRA_COUNT":("Number", int), 
        "TRA_AMT":("Number", float), 
        "EQU_TRA_AMT":("Number", float)}, # highlights destination table fields excluding reference_dates and control_dates; simply agg_columns and agg_measures
    "source_query" : """select tra_date, bra_code, cus_num, origt_bra_code, deb_cre_ind, expl_code, tra_amt, equ_tra_amt
                        from stg.src_transact t
                        where cus_num>99999
                        and can_rea_code=0
                        and expl_code in (1,2,25,34,641,642,952,955)
    """,
    "etl_period" : EtlPeriod.MONTHLY,
    "etl_period_freq" : 19,
    "source_control_date_field_name" : "TRA_DATE",
    "agg_columns" : ["bra_code", "cus_num", "origt_bra_code", "deb_cre_ind", "expl_code"], #optional
    "agg_measures" : [("count(1)", "tra_count"), ("sum(tra_amt)", "tra_amt"), ("sum(equ_tra_amt)", "equ_tra_amt")],
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

