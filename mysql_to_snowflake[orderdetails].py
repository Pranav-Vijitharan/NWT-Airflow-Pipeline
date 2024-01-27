from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mysql_to_snowflake_orderdetails',
    default_args=default_args,
    schedule_interval='0 22 * * *',
    start_date=datetime(2024, 1, 1),
)

def decide_next_task(**kwargs):
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(task_ids='extract_mysql_orderdetails_table')
    if extracted_data:
        return 'truncate_snowflake_orderdetails_table'
    else:
        return 'truncate_snowflake_orderdetails_table_end'


extract_mysql_orderdetails_table = MySqlOperator(
    task_id='extract_mysql_orderdetails_table',
    mysql_conn_id='NWT_Cloudbridge_OLTP',
    sql='SELECT * FROM nwt_cloudbridge.orderdetailsfresh',
    database='nwt_cloudbridge',
    dag=dag,
)

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=decide_next_task,
    provide_context=True,
    dag=dag,
)

truncate_snowflake_orderdetails_table = SnowflakeOperator(
    task_id='truncate_snowflake_orderdetails_table',
    sql='TRUNCATE TABLE NWT_DATA_GRP1.ADO_GRP1_ASG2.FRESH_ORDERDETAILS',
    snowflake_conn_id='NWT_Cloudbridge_OLAP',
    dag=dag,
)

truncate_snowflake_orderdetails_table_end = SnowflakeOperator(
    task_id='truncate_snowflake_orderdetails_table_end',
    sql='TRUNCATE TABLE NWT_DATA_GRP1.ADO_GRP1_ASG2.FRESH_ORDERDETAILS',
    snowflake_conn_id='NWT_Cloudbridge_OLAP',
    dag=dag,
)

load_orderdetails_table_snowflake = SnowflakeOperator(
    task_id='load_orderdetails_table_snowflake',
    snowflake_conn_id='NWT_Cloudbridge_OLAP',
    sql='''
    INSERT INTO NWT_DATA_GRP1.ADO_GRP1_ASG2.FRESH_ORDERDETAILS 
        (ORDERID, PRODUCTID, UNITPRICE, QUANTITY, DISCOUNT) 
    VALUES 
    {% for row in task_instance.xcom_pull(task_ids="extract_mysql_orderdetails_table") %}
        (
            CAST('{{ row[0] }}' AS NUMBER(38,0)), 
            CAST('{{ row[1] }}' AS NUMBER(38,0)), 
            CAST('{{ row[2] }}' AS FLOAT), 
            CAST('{{ row[3] }}' AS NUMBER(38,0)), 
            CAST('{{ row[4] }}' AS FLOAT)
        )
        {% if not loop.last %},{% endif %}
    {% endfor %}''',
    dag=dag,
)


truncate_mysql_orderdetails_table = MySqlOperator(
    task_id='truncate_mysql_orderdetails_table',
    sql='TRUNCATE TABLE nwt_cloudbridge.orderdetailsfresh',
    mysql_conn_id='NWT_Cloudbridge_OLTP',
    database='nwt_cloudbridge',
    dag=dag,
)


extract_mysql_orderdetails_table >> branch_task
branch_task >> [truncate_snowflake_orderdetails_table_end, truncate_snowflake_orderdetails_table] 
truncate_snowflake_orderdetails_table >> load_orderdetails_table_snowflake >> truncate_mysql_orderdetails_table