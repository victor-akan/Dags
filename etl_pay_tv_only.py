
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
    dag_id='air_etl_pay_tv',
    default_args=args,
    schedule_interval="@weekly",
    tags=['tv','pay'],
)

etl_args = {
    "dest_table_name" : "air_etl_pay_tv",
    "dest_table_columns_definition" : {
        "BRA_CODE":("Number(4)", int), 
        "CUS_NUM":("Number(7)", int), 
        "TRA_COUNT":("Number", int), 
        "TRA_AMT":("Number", float)}, # highlights destination table fields excluding reference_dates and control_dates; simply agg_columns and agg_measures
    "source_query" : """
        select bra_code, cus_num, tra_date, tra_amt
        from stg.src_transact
        where cus_num>99999  and deb_cre_ind = 1
        and tra_amt>100
        and 
        ( 
        -- GTCN 
        ( doc_alp = 'GTCN' and
        expl_code in (102,997) 
        and 
        (upper(remarks) like '%GOTV%'
        or upper(remarks) like '%QTBPWGTV%'
        or upper(remarks) like '%QTBPMGTV%'
        or upper(remarks) like '%DSTV%'
        or upper(remarks) like '%QTBPWSTIM%'
        or upper(remarks) like '%QTBPMSTIM%' 
        or upper(remarks) like '%STARTIME%'))
        or
        -- Web
        (expl_code = 470 and 
        ( 
        upper(remarks) like '%STARTIMES%'
        or  upper(remarks) like '%DSTV%' 
        or  upper(remarks) like '%GOTV%'
        or UPPER(remarks) like '%STARTIM/E APP%'  
        or UPPER(remarks) like '%ST/ARTIME%APP%'  
        or UPPER(remarks) like '%S TARTIMESA%P%'
        or UPPER(remarks) like '%STARTIM%SAP P%'  
        or UPPER(remarks) like '%STARTIM%/SAPP%'  
        or UPPER(remarks) like '%STARTIM E/APP%'
        or UPPER(remarks) like '%STARTIM/SAP P%'  
        or UPPER(remarks) like '%S TARTIME%APP%' 
        or UPPER(remarks) like '%ST ARTIME/APP%'  
        or UPPER(remarks) like '%S/ ARTIMESAPP%' 
        or UPPER(remarks) like '%S%ARTIMESA/PP%' 
        or UPPER(remarks) like '%STARTIM/ESA%P%' 
        or UPPER(remarks) like '%STARTIM%S APP%'  
        or UPPER(remarks) like '%ST ARTIMES%PP%'
        or UPPER(remarks) like '%ST/ARTIME APP%'  
        or UPPER(remarks) like '%S/%ARTIMESAPP%'  
        or UPPER(remarks) like '%S% ARTIMESAPP%'  
        or UPPER(remarks) like '%S/ARTIME SAPP%'  
        or UPPER(remarks) like '%STARTIM ESA/P%'  
        or UPPER(remarks) like '%STARTIM ES%PP%'  
        or UPPER(remarks) like '%STARTIM SA%PP%'  
        or UPPER(remarks) like '%S%ARTIMESAP P%'
        or UPPER(remarks) like '%S/ARTIMES%APP%'  
        or UPPER(remarks) like '%S TARTIME/APP%'  
        or UPPER(remarks) like '%S%TARTIMESA P%'  
        or UPPER(remarks) like '%STARTIM/E%APP%' 
        or UPPER(remarks) like '%S ARTIMESA%PP%'  
        or UPPER(remarks) like '%STARTIM%ESA P%'  
        or UPPER(remarks) like '%STARTIM E%APP%'  
        or UPPER(remarks) like '%STARTIM%E APP%'  
        or UPPER(remarks) like '%S ARTIMESAP/P%'  
        or UPPER(remarks) like '%ST/ARTIMES%PP%' 
        or UPPER(remarks) like '%S/TARTIMES PP%'  
        or UPPER(remarks) like '%S ARTIMESAP%P%'  
        or UPPER(remarks) like '%STARTIM/SAP%P%'  
        or UPPER(remarks) like '%STARTIM/ES PP%'  
        or UPPER(remarks) like '%STARTIM%ESA/P%' 
        or UPPER(remarks) like '%ST ARTIME%APP%'  
        or UPPER(remarks) like '%ST%ARTIMES/PP%'  
        or UPPER(remarks) like '%S%TARTIME APP%' 
        or UPPER(remarks) like '%S ARTIMES/APP%'  
        or UPPER(remarks) like '%ST ARTIMESA/P%'  
        or UPPER(remarks) like '%S TARTIMES/PP%' 
        or UPPER(remarks) like '%S%ARTIMES APP%' 
        or UPPER(remarks) like '%S/ARTIME%SAPP%' 
        or UPPER(remarks) like '%STARTIM SA/PP%' 
        or UPPER(remarks) like '%STARTIM%S/APP%' 
        or UPPER(remarks) like '%S%TARTIME/APP%' 
        or UPPER(remarks) like '%STARTIM /SAPP%'  
        or UPPER(remarks) like '%ST ARTIMESA%P%'  
        or UPPER(remarks) like '%S ARTIME/SAPP%'  
        or UPPER(remarks) like '%S ARTIME%SAPP%' 
        or UPPER(remarks) like '%S ARTIMESA/PP%'  
        or UPPER(remarks) like '%STARTIM %SAPP%'  
        or UPPER(remarks) like '%S TARTIMES%PP%'  
        or UPPER(remarks) like '%STARTIM%E/APP%'  
        or UPPER(remarks) like '%S/ARTIMESA PP%'  
        or UPPER(remarks) like '%S/ARTIMESAP P%'  
        or UPPER(remarks) like '%S%ARTIME/SAPP%'  
        or UPPER(remarks) like '%ST%ARTIME APP%'  
        or UPPER(remarks) like '%S%ARTIMESAP/P%'  
        or UPPER(remarks) like '%ST%ARTIMESA P%'  
        or UPPER(remarks) like '%S/ARTIMES APP%' 
        or UPPER(remarks) like '%S%ARTIME SAPP%'  
        or UPPER(remarks) like '%ST%ARTIME/APP%'  
        or UPPER(remarks) like '%S%ARTIMES/APP%' 
        or UPPER(remarks) like '%STARTIM/SA%PP%'  
        or UPPER(remarks) like '%S/ARTIMESA%PP%'  
        or UPPER(remarks) like '%STARTIM S%APP%'  
        or UPPER(remarks) like '%STARTIM%SAP/P%'  
        or UPPER(remarks) like '%ST/ARTIMES PP%'  
        or UPPER(remarks) like '%S %ARTIMESAPP%'  
        or UPPER(remarks) like '%STARTIM/%SAPP%'  
        or UPPER(remarks) like '%STARTIM S/APP%'  
        or UPPER(remarks) like '%ST ARTIMES/PP%'  
        or UPPER(remarks) like '%ST/ARTIMESA P%' 
        or UPPER(remarks) like '%STARTIM ES/PP%' 
        or UPPER(remarks) like '%S%TARTIMESA/P%'  
        or  UPPER(remarks) like '%STARTIM SAP%P%'  
        or  UPPER(remarks) like '%STARTIM SAP/P%'  
        or  UPPER(remarks) like '%S/TARTIMESA%P%' 
        or  UPPER(remarks) like '%S/TARTIME APP%'  
        or  UPPER(remarks) like '%ST%ARTIMESA/P%'  
        or  UPPER(remarks) like '%S%ARTIMESA PP%'  
        or  UPPER(remarks) like '%STARTIM%ES PP%'  
        or  UPPER(remarks) like '%S%TARTIMES/PP%'  
        or  UPPER(remarks) like '%STARTIM%SA PP%'  
        or  UPPER(remarks) like '%ST/ARTIMESA%P%'  
        or  UPPER(remarks) like '%STARTIM/SA PP%' 
        or  UPPER(remarks) like '%S/TARTIME%APP%'  
        or  UPPER(remarks) like '%S/TARTIMESA P%' 
        or  UPPER(remarks) like '%STARTIM/ES%PP%' 
        or UPPER(remarks) like '%S/TARTIMES%PP%' 
        or  UPPER(remarks) like '%STARTIM/S%APP%' 
        or  UPPER(remarks) like '%STARTIM% SAPP%'  
        or  UPPER(remarks) like '%S%/ARTIMESAPP%'  
        or  UPPER(remarks) like '%S /ARTIMESAPP%' 
        or  UPPER(remarks) like '%STARTIM/S APP%' 
        or  UPPER(remarks) like '%S/ARTIMESAP%P%' 
        or  UPPER(remarks) like '%STARTIM%SA/PP%'  
        or  UPPER(remarks) like '%S%TARTIMES PP%'  
        or  UPPER(remarks) like '%STARTIM%ES/PP%'  
        or  UPPER(remarks) like '%ST%ARTIMES PP%'  
        or  UPPER(remarks) like '%STARTIM/ESA P%' 
        or  UPPER(remarks) like '%S ARTIMES%APP%'  
        or UPPER(remarks) like '%STARTIM ESA%P%'  
        or UPPER(remarks) like '%STARTIM/ SAPP%' 
        or UPPER(remarks) like '%S TARTIMESA/P%'))

        or 
        -- Quickteller
        (expl_code in (1435,1436,1437)
        and    
        ( upper(remarks) like '%QTBPWGTV%'
        or upper(remarks) like '%QTBPMGTV%'
        or upper(remarks) like '%DSTV%'                                     
        or upper(remarks) like '%QTBPWSTIM%'
        or upper(remarks) like '%QTBPMSTIM%' 
        )))
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

