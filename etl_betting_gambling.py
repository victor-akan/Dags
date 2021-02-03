
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
    dag_id='air_etl_betting_gambling',
    default_args=args,
    schedule_interval="@weekly",
    tags=['betting','gambling'],
)

etl_args = {
    "dest_table_name" : "air_etl_betting_gambling",
    "dest_table_columns_definition" : {
        "BRA_CODE":("Number(4)", int), 
        "CUS_NUM":("Number(7)", int), 
        "TRA_COUNT":("Number", int), 
        "TRA_AMT":("Number", float)}, # highlights destination table fields excluding reference_dates and control_dates; simply agg_columns and agg_measures
    "source_query" : """
        select bra_code, cus_num, tra_date, tra_amt
        from stg.src_transact 
        where cus_num > 99999 and deb_cre_ind = 1
        and expl_code not in (44,157,643) and tra_amt > 100
        and
        (upper(remarks) like '%1960BET%'
        or upper(remarks) like '%360BET%'
        or upper(remarks) like '%ALPHAGRAM%WEST%AFRICA%'
        or upper(remarks) like '%BABA%IJEBU%'
        or upper(remarks) like '%BET9JA%'
        or upper(remarks) like '%BTOBET%' 
        or upper(remarks) like '%WESTERN%LOTTO%' 
        or upper(remarks) like '%BETBIGA%'
        or upper(remarks) like '%BETBONANZA%'
        or upper(remarks) like '%BETFARM%'
        or upper(remarks) like '%BETKING%' 
        or upper(remarks) like '%BETWAY%'
        or upper(remarks) like '%BETX%EXCHANGE%' 
        or upper(remarks) like '%BLACKBET%' 
        or upper(remarks) like '%CAMLAKE%LIMITED%' 
        or upper(remarks) like '%CHOPBARH%GAMING%' 
        or upper(remarks) like '%CRYSTAL%GAMING%' 
        or upper(remarks) like '%FORTUNEBET%' 
        or upper(remarks) like '%GAMESPAY%' 
        or upper(remarks) like '%GIVE%RAFFLE%' 
        or upper(remarks) like '%KC%GAMING%NETWORK%' 
        or upper(remarks) like '%KONFAM%BONUS%'
        or upper(remarks) like '%MEGABET%ENTERTAINMENT%' 
        or upper(remarks) like '%MERRYBET%GOLD%'
        or upper(remarks) like '%NAIRA%POWERBET%' 
        or upper(remarks) like '%NAIRABET%'
        or upper(remarks) like '%OVER%THE%TOP%ENTERTAIN%'
        or upper(remarks) like '%PLENTYMILLIONS%' 
        or upper(remarks) like '%PREMIER%LOTTO%' 
        or upper(remarks) like '%SPORTY%INTERNET%' 
        or upper(remarks) like '%SPORTYBET%' 
        or upper(remarks) like '%SUREBET247%' 
        or upper(remarks) like '%SV%GAMING%' 
        or upper(remarks) like '%WAKABET%' 
        or upper(remarks) like '%WESTERN%SPORT%BET%' 
        or upper(remarks) like '%WINNERS%BOOKMAKER%' 
        or upper(remarks) like '%WINNERS%GOLDEN%BET%' 
        or upper(remarks) like '%XTRAGOAL%FANTASY%')
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

