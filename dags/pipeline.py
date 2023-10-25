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
    'owner': 'workflow',
}

FILE_PATH = '/tmp/agents.csv'
FILE_COLS = ['Surname', 'Name', 'Count', 'Value', 'Branch', 'Location']

OUTPUT_FILE = '/Users/b/airflow/output/{}.csv'


def insert_branches_data():
    conn = psycopg2.connect(
        host="agent-db",
        database="agent_db",
        user="b",
        password="password"
    )
    
    cur = conn.cursor()
    
    for file in glob.glob(FILE_PATH):
        df = pd.read_csv(file, usecols=FILE_COLS) 
        df = df['Branch'].unique()
        df = pd.DataFrame(df, columns=['Branch'])
        
        records = df.to_dict('records')
        
        for record in records:
            query = f"""INSERT INTO branches 
                        (name) 
                        VALUES (
                            '{record['Branch']}')
                    """
            
            cur.execute(query)  
            
    conn.commit()
    
    cur.close()
    conn.close()
    
    
    
def insert_locations_data():
    conn = psycopg2.connect(
        host="agent-db",
        database="agent_db",
        user="b",
        password="password"
    )
    
    cur = conn.cursor()
    
    for file in glob.glob(FILE_PATH):
        df = pd.read_csv(file, usecols=FILE_COLS) 
        df = df['Location'].unique()
        df = pd.DataFrame(df, columns=['Location'])
        
        records = df.to_dict('records')
        
        for record in records:
            query = f"""INSERT INTO locations 
                        (name) 
                        VALUES (
                            '{record['Location']}')
                    """
            
            cur.execute(query)  
            
    conn.commit()
    
    cur.close()
    conn.close()
    

def insert_agents_data():
    conn = psycopg2.connect(
        host="agent-db",
        database="agent_db",
        user="b",
        password="password"
    )
    
    cur = conn.cursor()
    
    for file in glob.glob(FILE_PATH):
        df = pd.read_csv(file, usecols=FILE_COLS) 
        
        records = df.to_dict('records')
        
        for record in records:
            query = f"""INSERT INTO agents 
                        (surname, name, count, value, branch_id, location_id) 
                        VALUES (
                            '{record['Surname']}',
                            '{record['Name']}',
                            {record['Count']},
                            {record['Value']},
                            (SELECT id FROM branches WHERE name = '{record['Branch']}'),
                            (SELECT id FROM locations WHERE name = '{record['Location']}'))
                    """
            
            cur.execute(query)  
            
    conn.commit()
    
    cur.close()
    conn.close() 




# Define the DAG
with DAG (
    dag_id='new_agentpipeline_postgres',
    description='using TaskFlow API and Postgres',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['python', 'postgres'],
    template_searchpath='/sql_statements'
) as dag:
    
    create_table_branches = PostgresOperator(
        task_id='create_table_branches',
        postgres_conn_id='postgres_agent',
        sql='create_branch.sql'
    )
    
    create_table_locations = PostgresOperator(
        task_id='create_table_locations',
        postgres_conn_id='postgres_agent',
        sql='create_location.sql'
    )
    
    create_table_agents = PostgresOperator(
        task_id='create_table_agents',
        postgres_conn_id='postgres_agent',
        sql='create_agent.sql'
    )
    
    checking_for_file = FileSensor(
        task_id='checking_for_file',
        fs_conn_id='fs_default',
        filepath=FILE_PATH,
        poke_interval=10,
        timeout = 60 *10
    )
    
    insert_branches_data = PythonOperator(
        task_id='insert_branches_data',
        python_callable=insert_branches_data
    )
    
    insert_locations_data = PythonOperator(
        task_id='insert_locations_data',
        python_callable=insert_locations_data
    )
    
    insert_agents_data = PythonOperator(
        task_id='insert_agents_data',
        python_callable=insert_agents_data
    )
    
    
    create_table_branches >> create_table_locations >> create_table_agents >> \
        checking_for_file >> insert_branches_data >> insert_locations_data >> \
            insert_agents_data
        
            
        
        



