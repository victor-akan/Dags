from datetime import datetime,timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.oracle_operator import OracleOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago



default_arg = {'owner': 'Data Analytics', 'start_date': days_ago(2)}
  

dag = DAG(dag_id='air_etl_insight_customers',
          default_args=default_arg,
          schedule_interval="@daily",
          template_searchpath='/root/airflow/dags/',
          tags = ['insight'])


oracle_task = OracleOperator(dag=dag,
                        #    oracle_conn_id='mysql_default', 
                           task_id='oracle_task',
                           sql='/root/airflow/dags/customer_insights_query.sql',
                           autocommit ='True')

oracle_task








# tmpl_search_path = Variable.get("/root/airflow/dags/customer_insights_query.sql")


# dag = DAG(
#    'tutorial',
#     dag_id='air_etl_insight_customers',
#     schedule_interval="@daily",
#     template_searchpath=tmpl_search_path,  # this
#     default_args=default_args
# )

# def run_query():
#     # read the query
#     query = open('/root/airflow/dags/customer_insights_query.sql')
#     # run the query
#     execute(query)

# tas = PythonOperator(
#     task_id='run_query', dag=dag, python_callable=run_query)







