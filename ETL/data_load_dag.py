from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from clickhouse_driver import Client

def create_table():
    client = Client(host='clickhouse', port=9000, database='default')
    client.execute('''
        CREATE TABLE IF NOT EXISTS unprocessed_data (
            id String,
            datetime DateTime,
            city String,
            branch String,
            customer_type String,
            gender String,
            product_line String,
            unit_price Decimal(10,2),
            quantity Int32,
            tax Decimal(10,2),
            total Decimal(10,2),
            payment_method String,
            cogs Decimal(10,2),
            gross_margin_percentage Float32,
            gross_income Decimal(10,2),
            rating Float32,
            insert_time DateTime DEFAULT now()
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(datetime)
        ORDER BY (id, datetime)
    ''')
    client.disconnect()

def load_to_clickhouse():
    df = pd.read_csv('/opt/airflow/data/supermarket_sales - Sheet1.csv')
    df = df.rename(columns={
        'Invoice ID': 'id',
        'City': 'city',
        'Branch': 'branch',
        'Customer type': 'customer_type',
        'Gender': 'gender',
        'Product line': 'product_line',
        'Unit price': 'unit_price',
        'Quantity': 'quantity',
        'Tax 5%': 'tax',
        'Total': 'total',
        'Payment': 'payment_method',
        'gross margin percentage': 'gross_margin_percentage',
        'gross income': 'gross_income',
        'Rating': 'rating'
    })
    
    # Создаем колонку datetime из Date и Time
    df['datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
    df.drop(columns=['Date', 'Time'], inplace=True)
    
    # Добавляем колонку insert_time
    df['insert_time'] = datetime.now()
    
    client = Client(host='clickhouse', port=9000, database='default')
    client.execute("INSERT INTO unprocessed_data VALUES", df.to_dict('records'))
    client.disconnect()

with DAG(
    'data_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    start = EmptyOperator(task_id='start')
    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_to_clickhouse
    )
    end = EmptyOperator(task_id='end')

    start >> create_table_task >> load_task >> end