"""
 Airflow trials

"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

def try_branching():
    from airflow.models import Variable
    var = Variable.get("is_startml")
    if var =='True':
        return "startml_desc"
    else:
        return "not_startml_desc" 

def startml_desc():
    return "StartML is a starter course for ambitious people"

def not_startml_desc():
    return "Not a startML course, sorry" 

 
with DAG(
's-filonov-6_hw12',
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

    t1 = DummyOperator(
        task_id='before_branching',
    )

    branching = BranchPythonOperator(
        task_id='determine_course',
        python_callable=try_branching
    )

    choice1 = PythonOperator(
            task_id="startml_desc",
            python_callable= startml_desc
          )

    choice2 = PythonOperator(
            task_id="not_startml_desc",
            python_callable= not_startml_desc
          )

    t2 = DummyOperator(
        task_id='after_branching',
    )

    t1 >> branching >> [choice1, choice2] >> t2