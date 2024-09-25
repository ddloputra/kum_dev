from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from airflow.macros import datetime, timedelta

# Define the default arguments
default_args = {
    'start_date': datetime(2024, 9, 25),
    'depends_on_past': False,
    'retries': 1
}
# Define the DAG
dag = DAG(
    'etl_articles_postgres_to_warehouse',
    default_args=default_args,
    description='ETL process from Postgres source to Postgres warehouse',
    schedule_interval='@hourly',  # Runs every hour
)

def extract_data(**kwargs):
    src_pg_hook = PostgresHook(postgres_conn_id='postgres_source')
    sql = "SELECT * FROM articles"
    src_conn = src_pg_hook.get_conn()
    cursor = src_conn.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=columns)
    cursor.close()
    src_conn.close()
    return df.to_dict('records')

def load_data(**kwargs):
    ti = kwargs['ti']
    articles_data = ti.xcom_pull(task_ids='extract_data')
    
    if not articles_data:
        return 'No new data to load'
    
    dest_pg_hook = PostgresHook(postgres_conn_id='postgres_destination')
    dest_conn = dest_pg_hook.get_conn()
    cursor = dest_conn.cursor()
    
    for article in articles_data:
        insert_sql = """
        INSERT INTO articles (id, title, content, published_at, author_id, created_at, updated_at, deleted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) 
        DO UPDATE SET title = EXCLUDED.title,
                      content = EXCLUDED.content,
                      published_at = EXCLUDED.published_at,
                      author_id = EXCLUDED.author_id,
                      created_at = EXCLUDED.created_at,
                      updated_at = EXCLUDED.updated_at,
                      deleted_at = EXCLUDED.deleted_at
        """
        cursor.execute(insert_sql, (
            article['id'],
            article['title'],
            article['content'],
            article['published_at'],
            article['author_id'],
            article['created_at'],
            article['updated_at'],
            article['deleted_at']
        ))

    dest_conn.commit()
    cursor.close()
    dest_conn.close()
    return f"Loaded {len(articles_data)} articles into the warehouse"

# Task to extract data from the source database
extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

# Task to load data into the destination warehouse
load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
extract_data_task >> load_data_task
