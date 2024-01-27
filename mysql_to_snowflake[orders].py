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
    'mysql_to_snowflake_orders',
    default_args=default_args,
    schedule_interval='0 22 * * *',
    start_date=datetime(2024, 1, 1),
)

def decide_next_task(**kwargs):
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(task_ids='extract_mysql_orders_table')
    if extracted_data:
        return 'truncate_snowflake_orders_table'
    else:
        return 'truncate_snowflake_orders_table_end'


extract_mysql_orders_table = MySqlOperator(
    task_id='extract_mysql_orders_table',
    mysql_conn_id='NWT_Cloudbridge_OLTP',
    sql='SELECT * FROM nwt_cloudbridge.orderfresh WHERE shippedDate IS NOT NULL',
    database='nwt_cloudbridge',
    dag=dag,
)

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=decide_next_task,
    provide_context=True,
    dag=dag,
)

truncate_snowflake_orders_table = SnowflakeOperator(
    task_id='truncate_snowflake_orders_table',
    sql='TRUNCATE TABLE NWT_DATA_GRP1.ADO_GRP1_ASG2.FRESH_ORDERS',
    snowflake_conn_id='NWT_Cloudbridge_OLAP',
    dag=dag,
)

truncate_snowflake_orders_table_end = SnowflakeOperator(
    task_id='truncate_snowflake_orders_table_end',
    sql='TRUNCATE TABLE NWT_DATA_GRP1.ADO_GRP1_ASG2.FRESH_ORDERS',
    snowflake_conn_id='NWT_Cloudbridge_OLAP',
    dag=dag,
)

load_orders_table_snowflake = SnowflakeOperator(
    task_id='load_orders_table_snowflake',
    snowflake_conn_id='NWT_Cloudbridge_OLAP',
    sql='''
    INSERT INTO NWT_DATA_GRP1.ADO_GRP1_ASG2.FRESH_ORDERS 
        (ORDERID, CUSTOMERID, EMPLOYEEID, ORDERDATE, REQUIREDDATE, SHIPPEDDATE, SHIPVIA, FREIGHT, SHIPNAME, SHIPADDRESS, SHIPCITY, SHIPREGION, SHIPPOSTALCODE, SHIPCOUNTRY) 
    VALUES 
    {% for row in task_instance.xcom_pull(task_ids="extract_mysql_orders_table") %}
        (
            CAST('{{ row[0] }}' AS NUMBER(38,0)), 
            CAST('{{ row[1] }}' AS VARCHAR(16777216)), 
            CAST('{{ row[2] }}' AS NUMBER(38,0)), 
            CAST('{{ row[3] }}' AS TIMESTAMP_NTZ(9)), 
            CAST('{{ row[4] }}' AS TIMESTAMP_NTZ(9)), 
            CAST('{{ row[5] }}' AS TIMESTAMP_NTZ(9)), 
            CAST('{{ row[6] }}' AS NUMBER(38,0)), 
            CAST('{{ row[7] }}' AS NUMBER(38,0)), 
            CAST(
                {% if "'" in row[8] %}
                    '{{ row[8]|replace("'", "''") }}'
                {% else %}
                    '{{ row[8] }}'
                {% endif %}
            AS VARCHAR(16777216)), 
            CAST(
                {% if "'" in row[9] %}
                    '{{ row[9]|replace("'", "''") }}'
                {% else %}
                    '{{ row[9] }}'
                {% endif %}
            AS VARCHAR(16777216)), 
            CAST('{{ row[10] }}' AS VARCHAR(16777216)), 
            CAST('{{ row[11] }}' AS VARCHAR(16777216)), 
            CAST('{{ row[12] }}' AS VARCHAR(16777216)), 
            CAST('{{ row[13] }}' AS VARCHAR(16777216))
        )
        {% if not loop.last %},{% endif %}
    {% endfor %}''',
    dag=dag,
)

truncate_mysql_orders_table = MySqlOperator(
    task_id='truncate_mysql_orders_table',
    sql='DELETE FROM nwt_cloudbridge.orderfresh WHERE shippedDate IS NOT NULL;',
    mysql_conn_id='NWT_Cloudbridge_OLTP',
    database='nwt_cloudbridge',
    dag=dag,
)


extract_mysql_orders_table >> branch_task
branch_task >> [truncate_snowflake_orders_table_end, truncate_snowflake_orders_table]
truncate_snowflake_orders_table >> load_orders_table_snowflake >> truncate_mysql_orders_table