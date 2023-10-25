# from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import glob

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.decorators import task, dag


default_args = {
   'owner': 'workflow'
}

FILE_PATH = ['/tmp/laptops_01.csv', '/tmp/laptops_02.csv']
FILE_COLS = ['Id', 'Company', 'Product', 'TypeName', 'Price_euros']

OUTPUT_FILE = '/output/{}.csv'

def insert_laptop_data():
    conn = psycopg2.connect(
        host="agent-db",
        database="agent_db",
        user="b",
        password="password"
    )

    cur = conn.cursor()

    for file in FILE_PATH:
        df = pd.read_csv(file, usecols=FILE_COLS)

        records = df.to_dict('records')
        
        for record in records:
            query = f"""INSERT INTO laptops 
                        (id, company, product, type_name, price_euros) 
                        VALUES (
                            {record['Id']}, 
                            '{record['Company']}', 
                            '{record['Product']}', 
                            '{record['TypeName']}', 
                            {record['Price_euros']})
                    """

            cur.execute(query)

    conn.commit()

    cur.close()
    conn.close()

def filter_gaming_laptops():
    for file in FILE_PATH:

        df = pd.read_csv(file, usecols=FILE_COLS)
        
        gaming_laptops_df = df[df['TypeName'] == 'Gaming']
        
        gaming_laptops_df.to_csv(OUTPUT_FILE.format('gaming_laptops'), 
            mode='a', header=False, index=False)

def filter_notebook_laptops():
    for file in FILE_PATH:

        df = pd.read_csv(file, usecols=FILE_COLS)
        
        notebook_laptops_df = df[df['TypeName'] == 'Notebook']

        notebook_laptops_df.to_csv(OUTPUT_FILE.format('notebook_laptops'), 
            mode='a', header=False, index=False)

def filter_ultrabook_laptops():
    for file in FILE_PATH:

        df = pd.read_csv(file, usecols=FILE_COLS)
        
        ultrabook_laptops_df = df[df['TypeName'] == 'Ultrabook']

        ultrabook_laptops_df.to_csv(OUTPUT_FILE.format('ultrabook_laptops'), 
            mode='a', header=False, index=False)


with DAG(
    dag_id = 'pipeline_with_file_sensor',
    description = 'Running a pipeline using a file sensor',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['pipeline', 'sensor', 'file sensor'],
    template_searchpath = '/sql_statements'
) as dag:
    
    
    create_table_laptop = PostgresOperator(
        task_id = 'create_table_laptop',
        postgres_conn_id = 'postgres_agent',
        sql = 'create_laptop.sql'
    )


    checking_for_file = FileSensor(
        task_id = 'checking_for_file',
        fs_conn_id='fs_path',
        filepath = FILE_PATH[0],
        poke_interval = 10,
        timeout = 60 * 10
    )
    
    checking_for_file_2 = FileSensor(
    task_id = 'checking_for_file_2',
    fs_conn_id='fs_path_2',
    filepath = FILE_PATH[1],
    poke_interval = 10,
    timeout = 60 * 10
    )
    
    insert_laptop_data = PythonOperator(
        task_id = 'insert_laptop_data',
        python_callable = insert_laptop_data
    )

    filter_gaming_laptops = PythonOperator(
        task_id = 'filter_gaming_laptops',
        python_callable = filter_gaming_laptops
    )

    filter_notebook_laptops = PythonOperator(
        task_id = 'filter_notebook_laptops',
        python_callable = filter_notebook_laptops
    )

    filter_ultrabook_laptops = PythonOperator(
        task_id = 'filter_ultrabook_laptops',
        python_callable = filter_ultrabook_laptops
    )

    delete_file = BashOperator(
        task_id = 'delete_file',
        bash_command = 'rm {0}'.format(FILE_PATH)
    )
    

    create_table_laptop >> checking_for_file >> checking_for_file_2 >> insert_laptop_data >> \
    [filter_gaming_laptops, filter_notebook_laptops, filter_ultrabook_laptops] >> \
    delete_file


