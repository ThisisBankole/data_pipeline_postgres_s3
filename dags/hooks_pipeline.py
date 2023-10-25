import pandas as pd
import psycopg2
import glob

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.decorators import task, dag

from airflow.models import Variable

from airflow.providers.postgres.hooks.postgres import PostgresHook



default_args = {
    'owner': 'workflow',
}

employees_table = Variable.get("emp", default_var=None)
departments_table = Variable.get("dept", default_var=None)
employees_departments_table = Variable.get("emp_dept", default_var=None)



def create_employee_table():
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_agent')
    
    create_table_query = f"""CREATE TABLE IF NOT EXISTS {employees_table} (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        age INTEGER NOT NULL,
        department_id INTEGER NOT NULL
        );"""
        
    pg_hook.run(create_table_query)
    

def create_department_table():
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_agent')
    
    create_table_query = f"""CREATE TABLE IF NOT EXISTS {departments_table} (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL
        );"""
        
    pg_hook.run(create_table_query)
    
    
def insert_data_employees(employees):
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_agent')
    
    insert_query = f"""INSERT INTO {employees_table} (first_name, last_name, age, department_id) 
            VALUES (%s, %s, %s, %s)
    """
    
    for employee in employees:
        first_name, last_name, age, department_id = employee
        
        pg_hook.run(
            insert_query,
            parameters=(first_name, last_name, age, department_id)
        )
    
    
def insert_data_departments(departments):
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_agent')
    
    insert_query = f"""INSERT INTO {departments_table} (name)
            VALUES (%s)
    """
    
    
    for department in departments:
        name = department
        
        pg_hook.run(
            insert_query,
            parameters=(name,)
        )
    
    
def join_table():
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_agent')
    
    join_table_query = f"""
        CREATE TABLE IF NOT EXISTS {employees_departments_table} AS
        SELECT
            employees.first_name,
            employees.last_name,
            employees.age,
            departments.name AS department_name
        FROM employees JOIN departments 
        ON employees.department_id = departments.id
        
    """
    
    pg_hook.run(sql=join_table_query)

                    
def display_emp_dept():
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_agent')
    
    retrive_query = f"""
        SELECT * FROM {employees_departments_table}
    """
    
    results = pg_hook.get_records(retrive_query)
    
    for result in results:
        print(result)
        
        
def filtering_join_table(condition):
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_agent')
    
    retrive_query = f"""
        SELECT * FROM {employees_departments_table}
        WHERE department_name = '{condition}'
    """
    
    results = pg_hook.get_records(retrive_query)
    
    for result in results:
        print(result)
    
    
with DAG (
    dag_id='hook_pg',
    description='using hook and Postgres',
    default_args=default_args,
    start_date=days_ago(1),
    schedule=None,
    tags=['python', 'postgres'],
    
) as dag:
    
    create_employee_table_task = PythonOperator(
        task_id='create_employee_table',
        python_callable=create_employee_table
    )
    
    
    create_department_table_task = PythonOperator(
        task_id='create_department_table',
        python_callable=create_department_table
    )
    
    
    employees  = [
        ('John', 'Smith', 45, 1),
        ('Jane', 'Doe', 32, 2),
        ('Dave', 'Smith', 25, 2),
        ('Jack', 'Peterson', 36, 3),
        ('Mary', 'Johnson', 28, 3),
        ('Kate', 'Smith', 38, 1)
    ]
    
    insert_data_employees_task = PythonOperator(
        task_id='insert_data_employees',
        python_callable=insert_data_employees,
        op_kwargs={'employees': employees}
    )
    
    
    departments = ['Sales', 'Engineering', 'Marketing']
    
    
    insert_data_departments_task = PythonOperator(
        task_id='insert_data_departments',
        python_callable=insert_data_departments,
        op_kwargs={'departments': departments}
    )
    
    
    join_table_task = PythonOperator(
        task_id='join_table',
        python_callable=join_table
    )
    
    display_emp_dept_task = PythonOperator(
        task_id='display_emp_dept',
        python_callable=display_emp_dept
    )
    
    filtering_join_table_task = PythonOperator(
        task_id='filtering_join_table',
        python_callable=filtering_join_table,
        op_kwargs={'condition': 'Sales'}
    )
    
    
    
    create_employee_table_task >> insert_data_employees_task >>\
        join_table_task >> display_emp_dept_task >>\
        filtering_join_table_task
        
    
    create_department_table_task >> insert_data_departments_task >>\
        join_table_task >> display_emp_dept_task >>\
        filtering_join_table_task