
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
    'owner': 'Victor Akan',
    'start_date': datetime(2020,9,12)
}

dag = DAG(
    dag_id='air_etl_religious_phil',
    default_args=args,
    schedule_interval="@weekly",
    tags=['religious','philantropy'],
)

etl_args = {
    "dest_table_name" : "air_etl_religious_phil",
    "dest_table_columns_definition" : {
        "BRA_CODE":("Number(4)", int), 
        "CUS_NUM":("Number(7)", int), 
        "TRA_COUNT":("Number", int), 
        "TRA_AMT":("Number", float)}, # highlights destination table fields excluding reference_dates and control_dates; simply agg_columns and agg_measures
    "source_query" : """
    select bra_code, cus_num, tra_amt, tra_date from stg.src_transact
    where cus_num > 99999 and deb_cre_ind = 1 and expl_code not in (44,157)
    and
    (upper(remarks) like '%AFRICAN%ISLAMIC%ECONOMIC%' 
    or    upper(remarks) like '%ANSAR%UD%DEEN%' 
    or    upper(remarks) like '%ARCHBISHOP%' 
    or    upper(remarks) like '%ARCHDIOCESE%' 
    or    upper(remarks) like '%BENENOCH%DAVID%' 
    or    upper(remarks) like '%BREAK%FORTH%SEASON%' 
    or    upper(remarks) like '%BREAK%THR%' 
    or    upper(remarks) like '%CARE%GIVER%' 
    or    upper(remarks) like '%APOSTOLIC%'                                                                                                                       
    or    upper(remarks) like '%PRAYER%' 
    or    upper(remarks) like '%MOUNTAIN%'
    or    upper(remarks) like '%FASTING%' 
    or    upper(remarks) like '%COVENANT%' 
    or    upper(remarks) like '%DIVINE%REDEMPTION%ASSEMBLY%' 
    or    upper(remarks) like '%EGUNEC%EDUC%SUPPORT%' 
    or    upper(remarks) like '%EMPOWERED%EMPOWER%INITIAT%' 
    or    upper(remarks) like '%FAITH%ROCK%EVANGELICAL%' 
    or    upper(remarks) like '%FATHER%S%HOUSE%' 
    or    upper(remarks) like '%TRUTH%ASSEMBLY%' 
    or    upper(remarks) like '%FRUITFUL%HOUSE%BREAD%' 
    or    upper(remarks) like '%GOD%S%CHAMBER%' 
    or    upper(remarks) like '%GUIDING%LIGHT%ASSEMBLY%' 
    or    upper(remarks) like '%HOUR%OF%SALVATION%' 
    or    upper(remarks) like '%HOUSE%ON%THE%ROCK%' 
    or    upper(remarks) like '%K.I.C.C%MARYLAND%' 
    or    upper(remarks) like '%KELLY%BENEDICT%CHILD%' 
    or    upper(remarks) like '%LAKESHORE%CANCER%CLINIC%' 
    or    upper(remarks) like '%LIGHT%OF%GOD%INT%' 
    or    upper(remarks) like '%MARY%STARSEED%GLOBAL%FOUN%'
    or    upper(remarks) like '%MAVINTA%TENDER%HEART%' 
    or    upper(remarks) like '%PHOTIZO%LIFE%' 
    or    upper(remarks) like '%POSITIVE%FAITH%' 
    or    upper(remarks) like '%POTTER%S%HOUSE%' 
    or    upper(remarks) like '%PROJECT%AIM%MULTIPURPOSE%COOPE%' 
    or    upper(remarks) like '%ROCKFAITH%INT%TIAN%CENTRE%' 
    or    upper(remarks) like '%ROYAL%HOUSE%GRACE%' 
    or    upper(remarks) like '%SHEPHERD%VINE%' 
    or    upper(remarks) like '%SIMPLY%WORSHIP%INT%'
    or    upper(remarks) like '%ST%JOSEPH%CHOSEN%' 
    or    upper(remarks) like '%TETRA%EDGE%INT%' 
    or    upper(remarks) like '%THE%BAPTIZING%'
    or    upper(remarks) like '%THE%COMMUNION%' 
    or    upper(remarks) like '%THE%DREAM%CENTRE%LOIC%'
    or    upper(remarks) like '%THE%GOSPEL%FAITH%MISS%'
    or    upper(remarks) like '%THE%HEAVENLY%JERUSALEM%ASSEMBL%'
    or    upper(remarks) like '%THE%KING%ASSEMBLY%' 
    or    upper(remarks) like '%THIS%PRESENT%HOUSE%' 
    or    upper(remarks) like '%TRINITY%HOUSE%' 
    or    upper(remarks) like '%VOICE%ADONAI%MIN%INT%'
    or    upper(remarks) like '%WEST%AFRICA%THEO%SEMINARY%'
    or    upper(remarks) like '%WIL%WIL%ORPHANAGE%HOME%'
    or    upper(remarks) like '%WINNERS%CHAPEL%' 
    or    upper(remarks) like '%WORD%ASSEMBLY%' 
    or    upper(remarks) like '%CARE%GIVER%INITIATIVE%'
    or    upper(remarks) like '%HEALING%SCHOOL%'
    or    upper(remarks) like '%KELLY%BENEDICT%CHILD%INITIATIVE%' 
    or    upper(remarks) like '%MARY%STARSEED%GLOBAL%FOUNDATION%'
    or    upper(remarks) like '%PARISH%'
    or    upper(remarks) like '%SHILOH%ARENA%' 
    or    upper(remarks) like '%THE%LORD%SCEPTRE%'
    or    upper(remarks) like '%CITY%OF%DAVID%'
    or    upper(remarks) like '%GOD%MANIFOLD%' 
    or    upper(remarks) like '%KEY%OF%DAVID%'
    or    upper(remarks) like '%NAT%HQ%THRONE%GRACE%' 
    or    upper(remarks) like '%HEAVENLY%JERUSALEM%'
    or    upper(remarks) like '%WINNERS%CHAPEL%'
    or    upper(remarks) like '%RCCG%' 
    or    upper(remarks) like '%R.C.C.G%' 
    or    upper(remarks) like '%FIRST%FRUIT%' 
    or    upper(remarks) like '%TITHE%' 
    or    upper(remarks) like '%CHURCH%' 
    or    upper(remarks) like '%MOSQUE%' 
    or    upper(remarks) like '%OFFERING%' 
    or    upper(remarks) like '%REVIVAL%MIN%'
    or    upper(remarks) like '%REVIVAL%CHURCH%'
    or    upper(remarks) like '%REVIVAL%HOUSE%'    
    or    upper(remarks) like '%SOW%SEED%' 
    or    upper(remarks) like '%SEED%SOW%' 
    or    upper(remarks) like '%SPIRITUAL%SEED%' 
    or    upper(remarks) like '%CHRIST%CHURCH%'
    or    upper(remarks) like '%MINISTR%CHRIST%'
    or    upper(remarks) like '%CHRIST%CHAPEL%'
    or    upper(remarks) like '%CHRIST%APOSTOL%'
    or    upper(remarks) like '%EVANGELISM%'
    or    upper(remarks) like '%FOUNDATION%CHARITY%'
    or    upper(remarks) like '%CHARITY%FOUNDA%'
    or    upper(remarks) like '%CHARITY%ORGANIZATION%'
    or    upper(remarks) like '%MINISTRY%'
    or    upper(remarks) like '%COMMUNION%'
    or    upper(remarks) like '%HEALING%SCHOOL%'
    or    upper(remarks) like '%MINISTRIES%'
    or    upper(remarks) like '%ANGLICAN%' 
    or    upper(remarks) like '%CATHOLIC%'
    or    upper(remarks) like '%NASFAT%'
    or    upper(remarks) like '%PILGRIM%'
    or    upper(remarks) like '%OUTREACH%'
    or    upper(remarks) like '%ODUN IFA%'
    or    upper(remarks) like '%HOLY%GHOST%'
    or    upper(remarks) like '%HOLY%SPIRIT%'
    or    upper(remarks) like '%RITUAL%'
    or    upper(remarks) like '%WINNER%CHAPEL%'
    or    upper(remarks) like '%LIVING%FAITH%'
    or    upper(remarks) like '%DOMINION%CITY%'
    or    upper(remarks) like '%MFM%'
    or    upper(remarks) like '%ZAKAT%'
    or    upper(remarks) like '%JEHOVAH%WITNE%'
    or    upper(remarks) like '%EMPOWERMENT%'
    or    upper(remarks) like '%LOVE%FEAST%'
    or    upper(remarks) like '%CELESTIAL%'
    or    upper(remarks) like '%COCIN%')
    """,
    "etl_period" : EtlPeriod.MONTHLY,
    "etl_period_freq" : 12,
    "source_control_date_field_name" : "TRA_DATE",
    "agg_columns" : ["BRA_CODE","CUS_NUM"], # optional
    "agg_measures" : [
        ("COUNT(1)","TRA_COUNT"),
        ("NVL(SUM(TRA_AMT),0)", "TRA_AMT")],
    "optimize_extraction":False,
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

