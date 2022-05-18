"""
 Airflow trials

"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def get_user():
    """
    Connecting to a DB
    """
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
            SELECT user_id, COUNT(user_id)
            FROM feed_action 
            WHERE action = 'like'
            GROUP BY user_id
            ORDER BY COUNT(user_id) DESC
            LIMIT 1
            """)
            result = cursor.fetchone()
    return result 

with DAG(
's-filonov-6_hw10',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
},

description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 28),
    catchup=False,
    tags=['learning'],
) as dag:

    t1 = PythonOperator(
            task_id='get_user',
            python_callable= get_user,
          )

    t1 