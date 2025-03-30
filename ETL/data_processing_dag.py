from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
from clickhouse_driver import Client
import psycopg2
from psycopg2 import sql

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

clickhouse_params = {
    'host': 'clickhouse',
    'port': 9000,
    'database': 'default'
}

postgres_params = {
    'host': 'postgres',
    'port': 5432,
    'user': 'airflow',
    'password': 'airflow',
    'database': 'postgres'
}

column_map = {
    'cities': 'city',
    'branches': 'branch',
    'product_lines': 'product_line',
    'payment_methods': 'payment_method',
    'customer_types': 'customer_type'
}

POSTGRES_TABLES = {
    'nds': [
        "CREATE SCHEMA IF NOT EXISTS nds",
        "CREATE TABLE IF NOT EXISTS nds.cities (id SERIAL PRIMARY KEY, name VARCHAR(64) UNIQUE)",
        "CREATE TABLE IF NOT EXISTS nds.branches (id SERIAL PRIMARY KEY, name VARCHAR(64) UNIQUE, city_id INT REFERENCES nds.cities(id))",
        "CREATE TABLE IF NOT EXISTS nds.product_lines (id SERIAL PRIMARY KEY, name VARCHAR(64) UNIQUE)",
        "CREATE TABLE IF NOT EXISTS nds.payment_methods (id SERIAL PRIMARY KEY, name VARCHAR(64) UNIQUE)",
        "CREATE TABLE IF NOT EXISTS nds.customer_types (id SERIAL PRIMARY KEY, name VARCHAR(64) UNIQUE)",
        """
        CREATE TABLE IF NOT EXISTS nds.sales (
            id VARCHAR(11) PRIMARY KEY,
            datetime TIMESTAMP,
            city_id INT REFERENCES nds.cities(id),
            branch_id INT REFERENCES nds.branches(id),
            customer_type_id INT REFERENCES nds.customer_types(id),
            gender VARCHAR(8),
            payment_method_id INT REFERENCES nds.payment_methods(id),
            product_line_id INT REFERENCES nds.product_lines(id),
            unit_price FLOAT,
            quantity INT,
            tax DECIMAL(10,2),
            total DECIMAL(10,2),
            rating FLOAT
        )
        """
    ],
    'dds': [
        "CREATE SCHEMA IF NOT EXISTS dds",
        "CREATE TABLE IF NOT EXISTS dds.cities (id SERIAL PRIMARY KEY, name VARCHAR(64))",
        "CREATE TABLE IF NOT EXISTS dds.branches (id SERIAL PRIMARY KEY, name VARCHAR(64), city VARCHAR(64))",
        "CREATE TABLE IF NOT EXISTS dds.product_lines (id SERIAL PRIMARY KEY, name VARCHAR(64))",
        "CREATE TABLE IF NOT EXISTS dds.payment_methods (id SERIAL PRIMARY KEY, name VARCHAR(64))",
        "CREATE TABLE IF NOT EXISTS dds.customer_types (id SERIAL PRIMARY KEY, name VARCHAR(64))",
        "CREATE TABLE IF NOT EXISTS dds.sales_dates (id SERIAL PRIMARY KEY, date DATE, year INT, month INT, month_name VARCHAR(9), day INT, day_of_the_week VARCHAR(9))",
        "CREATE TABLE IF NOT EXISTS dds.sales_time (id SERIAL PRIMARY KEY, datetime TIMESTAMP, hour INT, minute INT, second INT)",
        "CREATE TABLE IF NOT EXISTS dds.sales_rating (id SERIAL PRIMARY KEY, rating FLOAT)",
        """
        CREATE TABLE IF NOT EXISTS dds.sales (
            id VARCHAR(11) PRIMARY KEY,
            date_id INT REFERENCES dds.sales_dates(id),
            time_id INT REFERENCES dds.sales_time(id),
            city VARCHAR(64),
            branch VARCHAR(64),
            customer_type VARCHAR(64),
            gender VARCHAR(8),
            payment_method VARCHAR(64),
            product_line VARCHAR(64),
            rating_id INT REFERENCES dds.sales_rating(id),
            unit_price FLOAT,
            quantity INT,
            tax DECIMAL(10,2),
            total DECIMAL(10,2),
            cogs DECIMAL(10,2),
            gross_income DECIMAL(10,2)
        )
        """
    ]
}

REFERENCE_DATA = {
    'nds': {
        'cities': ['Yangon', 'Mandalay', 'Naypyitaw'],
        'branches': [('A', 1), ('B', 2), ('C', 3)],
        'product_lines': ['Sports and travel', 'Food and beverages', 'Health and beauty',
                         'Fashion accessories', 'Electronic accessories', 'Home and lifestyle'],
        'customer_types': ['Normal', 'Member'],
        'payment_methods': ['Cash', 'Credit card', 'Ewallet']
    },
    'dds': {
        'cities': ['Yangon', 'Mandalay', 'Naypyitaw'],
        'branches': [('A', 'Yangon'), ('B', 'Mandalay'), ('C', 'Naypyitaw')],
        'product_lines': ['Sports and travel', 'Food and beverages', 'Health and beauty',
                         'Fashion accessories', 'Electronic accessories', 'Home and lifestyle'],
        'customer_types': ['Normal', 'Member'],
        'payment_methods': ['Cash', 'Credit card', 'Ewallet']
    }
}

def create_postgres_tables(**kwargs):
    """Создание таблиц и заполнение справочников в PostgreSQL"""
    conn = psycopg2.connect(**kwargs['postgres_params'])
    cursor = conn.cursor()
    try:
        # Создание схем
        cursor.execute("CREATE SCHEMA IF NOT EXISTS nds")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS dds")
        
        # Создание таблиц для NDS
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nds.cities (
                id SERIAL PRIMARY KEY,
                name VARCHAR(64) UNIQUE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nds.branches (
                id SERIAL PRIMARY KEY,
                name VARCHAR(64) UNIQUE,
                city_id INT REFERENCES nds.cities(id)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nds.product_lines (
                id SERIAL PRIMARY KEY,
                name VARCHAR(64) UNIQUE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nds.payment_methods (
                id SERIAL PRIMARY KEY,
                name VARCHAR(64) UNIQUE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nds.customer_types (
                id SERIAL PRIMARY KEY,
                name VARCHAR(64) UNIQUE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nds.sales (
                id VARCHAR(11) PRIMARY KEY,
                datetime TIMESTAMP,
                city_id INT REFERENCES nds.cities(id),
                branch_id INT REFERENCES nds.branches(id),
                customer_type_id INT REFERENCES nds.customer_types(id),
                gender VARCHAR(8),
                payment_method_id INT REFERENCES nds.payment_methods(id),
                product_line_id INT REFERENCES nds.product_lines(id),
                unit_price FLOAT,
                quantity INT,
                tax DECIMAL(10,2),
                total DECIMAL(10,2),
                rating FLOAT
            )
        """)

        # Создание таблиц для DDS
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dds.cities (
                id SERIAL PRIMARY KEY,
                name VARCHAR(64)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dds.branches (
                id SERIAL PRIMARY KEY,
                name VARCHAR(64),
                city VARCHAR(64)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dds.product_lines (
                id SERIAL PRIMARY KEY,
                name VARCHAR(64)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dds.payment_methods (
                id SERIAL PRIMARY KEY,
                name VARCHAR(64)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dds.customer_types (
                id SERIAL PRIMARY KEY,
                name VARCHAR(64)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dds.sales_dates (
                id SERIAL PRIMARY KEY,
                date DATE,
                year INT,
                month INT,
                month_name VARCHAR(9),
                day INT,
                day_of_the_week VARCHAR(9)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dds.sales_time (
                id SERIAL PRIMARY KEY,
                datetime TIMESTAMP,
                hour INT,
                minute INT,
                second INT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dds.sales_rating (
                id SERIAL PRIMARY KEY,
                rating FLOAT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dds.sales (
                id VARCHAR(11) PRIMARY KEY,
                date_id INT REFERENCES dds.sales_dates(id),
                time_id INT REFERENCES dds.sales_time(id),
                city VARCHAR(64),
                branch VARCHAR(64),
                customer_type VARCHAR(64),
                gender VARCHAR(8),
                payment_method VARCHAR(64),
                product_line VARCHAR(64),
                rating_id INT REFERENCES dds.sales_rating(id),
                unit_price FLOAT,
                quantity INT,
                tax DECIMAL(10,2),
                total DECIMAL(10,2),
                cogs DECIMAL(10,2),
                gross_income DECIMAL(10,2)
            )
        """)
        
        # Заполнение справочников
        for city in REFERENCE_DATA['nds']['cities']:
            cursor.execute("INSERT INTO nds.cities (name) VALUES (%s) ON CONFLICT DO NOTHING", (city,))
        
        for branch, city_id in REFERENCE_DATA['nds']['branches']:
            cursor.execute("INSERT INTO nds.branches (name, city_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", 
                         (branch, city_id))
        
        for name in REFERENCE_DATA['nds']['product_lines']:
            cursor.execute("INSERT INTO nds.product_lines (name) VALUES (%s) ON CONFLICT DO NOTHING", (name,))
        
        for name in REFERENCE_DATA['nds']['customer_types']:
            cursor.execute("INSERT INTO nds.customer_types (name) VALUES (%s) ON CONFLICT DO NOTHING", (name,))
        
        for name in REFERENCE_DATA['nds']['payment_methods']:
            cursor.execute("INSERT INTO nds.payment_methods (name) VALUES (%s) ON CONFLICT DO NOTHING", (name,))
        
        # Заполнение DDS справочников
        for city in REFERENCE_DATA['dds']['cities']:
            cursor.execute("INSERT INTO dds.cities (name) VALUES (%s) ON CONFLICT DO NOTHING", (city,))
        
        for branch, city in REFERENCE_DATA['dds']['branches']:
            cursor.execute("INSERT INTO dds.branches (name, city) VALUES (%s, %s) ON CONFLICT DO NOTHING", 
                         (branch, city))
        
        for name in REFERENCE_DATA['dds']['product_lines']:
            cursor.execute("INSERT INTO dds.product_lines (name) VALUES (%s) ON CONFLICT DO NOTHING", (name,))
        
        for name in REFERENCE_DATA['dds']['customer_types']:
            cursor.execute("INSERT INTO dds.customer_types (name) VALUES (%s) ON CONFLICT DO NOTHING", (name,))
        
        for name in REFERENCE_DATA['dds']['payment_methods']:
            cursor.execute("INSERT INTO dds.payment_methods (name) VALUES (%s) ON CONFLICT DO NOTHING", (name,))
        
        conn.commit()
        print("Таблицы и справочники в PostgreSQL созданы/обновлены")
    finally:
        cursor.close()
        conn.close()

def create_clickhouse_tables(**kwargs):
    client = Client(**kwargs['clickhouse_params'])
    try:
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
                gross_income Decimal(10,2),
                rating Float32
            ) ENGINE = MergeTree 
            PARTITION BY toYYYYMM(datetime) 
            ORDER BY (id, datetime)
        ''')
        
        client.execute('''
            CREATE TABLE IF NOT EXISTS processed_data (
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
                gross_income Decimal(10,2),
                rating Float32
            ) ENGINE = MergeTree 
            PARTITION BY toYYYYMM(datetime) 
            ORDER BY (id, datetime)
        ''')
    finally:
        client.disconnect()

def extract_unprocessed_data(**kwargs):
    client = Client(**kwargs['clickhouse_params'])
    df = pd.DataFrame()
    try:
        tables = client.execute("SHOW TABLES FROM default")
        if 'unprocessed_data' not in [t[0] for t in tables]:
            raise ValueError("Таблица unprocessed_data не найдена")
        
        # ЯВНОЕ УКАЗАНИЕ КОЛОНКИ ЧЕРЕЗ SELECT
        data = client.execute(
            """
            SELECT id, datetime, city, branch, customer_type, gender, 
                   product_line, unit_price, quantity, tax, total, 
                   payment_method, cogs, gross_income, rating 
            FROM unprocessed_data
            """
        )
        
        columns = [
            'id', 'datetime', 'city', 'branch', 'customer_type', 
            'gender', 'product_line', 'unit_price', 'quantity', 
            'tax', 'total', 'payment_method', 'cogs', 'gross_income', 'rating'
        ]
        
        df = pd.DataFrame(data, columns=columns)
        
        # Дополнительная проверка
        for col in ['insert_time', 'gross_margin_percentage']:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)
        
        print("Колонки после извлечения:", df.columns.tolist())
        kwargs['ti'].xcom_push(key='clean_data', value=df)
    finally:
        client.disconnect()
    return df

def dataframe_values_mapping(postgres_params, schema, column_map, df):
    conn = psycopg2.connect(**postgres_params)
    cursor = conn.cursor()
    try:
        for table_name, column in column_map.items():
            cursor.execute(
                sql.SQL("SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = %s AND tablename = %s)"),
                (schema, table_name)
            )
            exists = cursor.fetchone()[0]
            if not exists:
                raise ValueError(f"Таблица {schema}.{table_name} не существует")
            
            cursor.execute(
                sql.SQL("SELECT name, id FROM {}.{}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name)
                )
            )
            mapping = {name: id_ for id_, name in cursor.fetchall()}
            if not mapping:
                raise ValueError(f"Справочник {schema}.{table_name} пуст")
            
            df[column] = df[column].map(mapping).fillna(0).astype(int)
        return df
    finally:
        cursor.close()
        conn.close()

def insert_dates(cursor, row, schema):
    cursor.execute(
        sql.SQL("""
            INSERT INTO {schema}.sales_dates (date, year, month, month_name, day, day_of_the_week)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """).format(schema=sql.Identifier(schema)),
        (
            row['datetime'].date(),
            row['datetime'].year,
            row['datetime'].month,
            row['datetime'].strftime('%B'),
            row['datetime'].day,
            row['datetime'].strftime('%A')
        )
    )
    return cursor.fetchone()[0]

def insert_time(cursor, row, schema):
    cursor.execute(
        sql.SQL("""
            INSERT INTO {schema}.sales_time (datetime, hour, minute, second)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """).format(schema=sql.Identifier(schema)),
        (
            row['datetime'],
            row['datetime'].hour,
            row['datetime'].minute,
            row['datetime'].second
        )
    )
    return cursor.fetchone()[0]

def insert_rating(cursor, row, schema):
    cursor.execute(
        sql.SQL("INSERT INTO {schema}.sales_rating (rating) VALUES (%s) RETURNING id").format(
            schema=sql.Identifier(schema)),
        (row['rating'],)
    )
    return cursor.fetchone()[0]

def load_to_nds(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='extract_unprocessed_data', key='clean_data')
    mapped_df = dataframe_values_mapping(
        kwargs['postgres_params'], 
        'nds', 
        column_map, 
        df.copy()
    )
    
    conn = psycopg2.connect(**kwargs['postgres_params'])
    cursor = conn.cursor()
    try:
        for _, row in mapped_df.iterrows():
            cursor.execute(
                sql.SQL("""
                    INSERT INTO nds.sales (
                        id, datetime, city_id, branch_id, 
                        customer_type_id, gender, 
                        payment_method_id, product_line_id, 
                        unit_price, quantity, tax, total, rating
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """),
                (
                    row['id'], row['datetime'], row['city'], row['branch'], 
                    row['customer_type'], row['gender'], 
                    row['payment_method'], row['product_line'],
                    row['unit_price'], row['quantity'], 
                    row['tax'], row['total'], row['rating']
                )
            )
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def load_to_dds(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='extract_unprocessed_data', key='clean_data')
    mapped_df = dataframe_values_mapping(
        kwargs['postgres_params'], 
        'dds', 
        column_map, 
        df.copy()
    )
    
    conn = psycopg2.connect(**kwargs['postgres_params'])
    cursor = conn.cursor()
    try:
        for _, row in mapped_df.iterrows():
            date_id = insert_dates(cursor, row, 'dds')
            time_id = insert_time(cursor, row, 'dds')
            rating_id = insert_rating(cursor, row, 'dds')
            
            cursor.execute(
                sql.SQL("""
                    INSERT INTO dds.sales (
                        id, date_id, time_id, city, branch, 
                        customer_type, gender, payment_method, 
                        product_line, rating_id, 
                        unit_price, quantity, tax, total, 
                        cogs, gross_income
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """),
                (
                    row['id'], date_id, time_id, 
                    row['city'], row['branch'], 
                    row['customer_type'], row['gender'],
                    row['payment_method'], row['product_line'],
                    rating_id,
                    row['unit_price'], row['quantity'], 
                    row['tax'], row['total'],
                    row['cogs'], row['gross_income']
                )
            )
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def transfer_of_processed_data(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='extract_unprocessed_data', key='clean_data')
    
    # Удаление всех возможных проблемных колонок
    columns_to_drop = ['insert_time', 'gross_margin_percentage']
    for col in columns_to_drop:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    # Фильтрация только нужных колонок
    expected_columns = [
        'id', 'datetime', 'city', 'branch', 'customer_type', 'gender',
        'product_line', 'unit_price', 'quantity', 'tax', 'total',
        'payment_method', 'cogs', 'gross_income', 'rating'
    ]
    df = df[expected_columns]

    # Проверка типов данных (пример для datetime)
    df['datetime'] = pd.to_datetime(df['datetime'])

    # Отладочный вывод
    print("Столбцы перед вставкой:", df.columns.tolist())
    print("Пример данных:", df.head(2).to_dict())

    # Вставка с явным указанием колонок
    client = Client(**kwargs['clickhouse_params'])
    try:
        client.execute(
            """
            INSERT INTO processed_data (
                id, datetime, city, branch, customer_type, gender,
                product_line, unit_price, quantity, tax, total,
                payment_method, cogs, gross_income, rating
            ) VALUES
            """, 
            df.to_dict('records')
        )
        client.execute("TRUNCATE TABLE unprocessed_data")
    except Exception as e:
        print(f"Ошибка при вставке: {str(e)}")
        raise
    finally:
        client.disconnect()

with DAG(
    'data_processing_dag',
    default_args=default_args,
    schedule="@daily",
    catchup=False
) as dag:

    start = EmptyOperator(task_id='start')

    create_tables = PythonOperator(
        task_id='create_postgres_tables',
        python_callable=create_postgres_tables,
        op_kwargs={'postgres_params': postgres_params}
    )

    create_clickhouse = PythonOperator(
        task_id='create_clickhouse_tables',
        python_callable=create_clickhouse_tables,
        op_kwargs={'clickhouse_params': clickhouse_params}
    )

    extract_data = PythonOperator(
        task_id='extract_unprocessed_data',
        python_callable=extract_unprocessed_data,
        op_kwargs={'clickhouse_params': clickhouse_params}
    )

    load_nds = PythonOperator(
        task_id='load_to_nds',
        python_callable=load_to_nds,
        op_kwargs={
            'postgres_params': postgres_params,
            'column_map': column_map
        }
    )

    load_dds = PythonOperator(
        task_id='load_to_dds',
        python_callable=load_to_dds,
        op_kwargs={
            'postgres_params': postgres_params,
            'column_map': column_map
        }
    )

    transfer_data = PythonOperator(
        task_id='transfer_of_processed_data',
        python_callable=transfer_of_processed_data,
        op_kwargs={'clickhouse_params': clickhouse_params}
    )

    end = EmptyOperator(task_id='end')

    start >> create_tables >> create_clickhouse >> extract_data
    extract_data >> [load_nds, load_dds] >> transfer_data >> end