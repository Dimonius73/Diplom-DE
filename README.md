# Дипломная работа по курсу "Дата - инженер"

![](https://github.com/Dimonius73/Diplom-DE/blob/main/Изображения/zast.png)
---
## Содержание  
- [1. Введение](#introduction)  
- [2. Анализ данных](#first-stage)  
- [3. Формирование состава таблиц, построение ER-диаграмм и DDL-запросов](#second-stage)  
- [4. Разработка ETL-процессов](#third-stage)  
- [5. Формирование набора метрик и визуализация данных](#fourth-stage)  
- [6. Выводы](#conclusions)  

## <a name="introduction"></a> 1. Введение

В современном информационном мире организации сталкиваются с огромным объемом данных, требующих систематизации, хранения и анализа. Однако, с ростом объемов информации и ее многообразием, эффективное управление данными становится ключевым аспектом успешной деятельности предприятий. В этом контексте концепция DataOps приобретает особую актуальность, объединяя в себе лучшие практики DevOps и принципы управления данными.

Целью данной дипломной работы является разработка и документирование процессов ETL (Extract, Transform, Load) для создания и поддержки хранилища данных. Это хранилище состоит из двух основных слоев: нормализованного хранилища (NDS) и схемы звезда (DDS). Этот процесс обеспечит эффективное хранение и управление данными, что в свою очередь способствует принятию обоснованных бизнес-решений.

На завершающем этапе работы, на основе данных из схемы звезда (DDS), разработан дашборд, предоставляющие наглядное представление ключевых аспектов бизнеса.

Для реализации дипломной работы использовались:
- Дистрибутив Anaconda (Jupiter Lab) - для анализа данных
- Сервис [dbdiagram.io](https://dbdiagram.io/) - для построения ER-диаграмм
- ClickHouse и PostgreSQL (Docker) - для хранения данных
- Apache Airflow (Docker) - для ETL-процессов
- Power BI Desktop - для построения дашборда

---

## <a name="first-stage"></a> 2. Анализ данных

[Датасет](https://github.com/Dimonius73/Diplom-DE/blob/main/supermarket_sales%20-%20Sheet1.csv)

**Контекст датасета:** датасет `Supermarket Sales` представляет из себя срез исторических данных о продажах товаров в 3 филиалах компании за 3 месяца.

**Атрибуты:**
1. `Invoice ID`: програмно-генерируемый идентификационный номер счета-фактуры
2. `Branch`: название филиала компании
3. `City`: местонахождение филиала (город)
4. `Customer Type`: тип покупателя (наличие клубной карты)
5. `Gender`: пол покупателя
6. `Product Line`: продуктовая линейка
7. `Unit Price`: цена единицы товара в долларах
8. `Quantity`: количество проданных товаров
9. `Tax`: сумма взимаемого налога с продажи (5%)
10. `Total`: общая стоимость продажи, включая налоги
11. `Date`: дата продажи
12. `Time`: время продажи
13. `Payment`: метод оплаты
14. `COGS`: себестоимость проданных товаров
15. `Gross Profit Percentage`: процент прибыли
16. `Gross Revenue`: прибыль с продажи
17. `Rating`: рейтинг покупки от покупателя (по шкале от 1 до 10)

Для анализа данных воспользуемся библиотеками `pandas`, `matplotlib` и `seaborn`.

[Colaboratory](https://github.com/Dimonius73/Diplom-DE/blob/main/Diplom_DE.ipynb)

**Загрузка данных в Pandas DataFrame:**

```python
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
```

```python
df = pd.read_csv('supermarket_sales - Sheet1.csv', delimiter=',')
print(df.head(3))
```

```
    Invoice ID Branch       City Customer type  Gender  \
0  750-67-8428      A     Yangon        Member  Female   
1  226-31-3081      C  Naypyitaw        Normal  Female   
2  631-41-3108      A     Yangon        Normal    Male   

             Product line  Unit price  Quantity   Tax 5%     Total      Date  \
0       Health and beauty       74.69         7  26.1415  548.9715  1/5/2019   
1  Electronic accessories       15.28         5   3.8200   80.2200  3/8/2019   
2      Home and lifestyle       46.33         7  16.2155  340.5255  3/3/2019   

    Time      Payment    cogs  gross margin percentage  gross income  Rating  
0  13:08      Ewallet  522.83                 4.761905       26.1415     9.1  
1  10:29         Cash   76.40                 4.761905        3.8200     9.6  
2  13:23  Credit card  324.31                 4.761905       16.2155     7.4  
```

Данные успешно загружены.

**Проверка типов данных, количества строк и Null значений:**
```python
df.info()
```

```
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 1000 entries, 0 to 999
Data columns (total 17 columns):
 #   Column                   Non-Null Count  Dtype  
---  ------                   --------------  -----  
 0   Invoice ID               1000 non-null   object 
 1   Branch                   1000 non-null   object 
 2   City                     1000 non-null   object 
 3   Customer type            1000 non-null   object 
 4   Gender                   1000 non-null   object 
 5   Product line             1000 non-null   object 
 6   Unit price               1000 non-null   float64
 7   Quantity                 1000 non-null   int64  
 8   Tax 5%                   1000 non-null   float64
 9   Total                    1000 non-null   float64
 10  Date                     1000 non-null   object 
 11  Time                     1000 non-null   object 
 12  Payment                  1000 non-null   object 
 13  cogs                     1000 non-null   float64
 14  gross margin percentage  1000 non-null   float64
 15  gross income             1000 non-null   float64
 16  Rating                   1000 non-null   float64
dtypes: float64(7), int64(1), object(9)
memory usage: 132.9+ KB
```

В датафрейме 1000 строк, Null значения отсутствуют.

**Проверка строк на дубликаты:**
```python
df.duplicated().any()
```

```
False
```

Дубликаты отсутствуют.

**Вывод списков представленных филиалов, городов, типов клиентов, методов оплаты и продуктовых линейках:**

```python
print(df['Branch'].unique())
print(df['City'].unique())
print(df['Customer type'].unique())
print(df['Gender'].unique())
print(df['Product line'].unique())
print(df['Payment'].unique())
```

```
['A' 'C' 'B']
['Yangon' 'Naypyitaw' 'Mandalay']
['Member' 'Normal']
['Female' 'Male']
['Health and beauty' 'Electronic accessories' 'Home and lifestyle'
 'Sports and travel' 'Food and beverages' 'Fashion accessories']
['Ewallet' 'Cash' 'Credit card']
```

Категориальные переменные определены.

**Вывод статистики по всем столбцам датафрейма:**

```
df.describe(include='all')
```
![](https://github.com/Dimonius73/Diplom-DE/blob/main/Изображения/01-describe.png)

Из полученной статистики узнаем:
  1. В данных почти равное распределение как по типу клиента (*Member* / *Normal*), так и по полу (*Female* / *Male*). В обоих случаях это 501 на 499.
  2. Наибольшей популярностью пользуется категория *Fashion accessories*. На нее приходится 178 продаж.
  3. Средняя стоимость единицы товара $55.67.
  4. Среднее количество товаров в покупке 5.51.
  5. Средний налог с продажи $15.38.
  6. Средних доход с продажи $322.97.
  7. Средняя прибыль с продажи $15.38.
  8. Средний рейтинг покупки 6.97 балов.

**Построение столбчатой диаграммы по распределению продаж на филиалы:**

```python
# группировка данных
branch_stat = df.groupby('Branch').nunique().reset_index()

# построение графика
plt.figure(figsize=(3, 4))
sns.set(style="whitegrid")
sns.barplot(x='Branch', y='Invoice ID', data=branch_stat, palette="viridis")

plt.title('Количество продаж по филиалам')
plt.xlabel('Филиал')
plt.ylabel('Количество продаж')

plt.show()
```
![](https://github.com/Dimonius73/Diplom-DE/blob/main/Изображения/02-branch.png)
Из диаграммы видно, что больше всего продаж приходится на филиал *A*, но в целом распределение почти равное.

**Подсчет средней разницы в количестве продаж между филиалами:**

```python
average_percentage_difference = (branch_stat['Invoice ID'].max() - branch_stat['Invoice ID'].min()) / branch_stat['Invoice ID'].mean() * 100
print('Разница составляет: ', average_percentage_difference.round(3), '%')
```

```
Разница составляет: 3.6 %
```

**Распределение продаж по продуктовым линейкам:**

```python
product_line_stat = df.groupby('Product line')['Invoice ID'].nunique().reset_index()

plt.figure(figsize=(4, 4))
sns.set(style="whitegrid")
sns.barplot(x='Product line', y='Invoice ID', data=product_line_stat, palette="viridis")

plt.title('Количество продаж по категориям товаров')
plt.xlabel('Категория товаров')
plt.ylabel('Количество продаж')
plt.xticks(rotation=45, ha='center')

plt.show()
```
![](https://github.com/Dimonius73/Diplom-DE/blob/main/Изображения/03-категория.png)

Из полученной диаграммы видно, что наибольшее число продаж приходится на категорию *Fashion Accessories*, наименьшее на категорию *Health and beauty*.

**Построение тепловой карты (город - метод оплаты):**

```python
sns.heatmap(pd.crosstab(df['City'], df['Payment']), cmap="YlGnBu")

plt.title('Тепловая карта по городам и методам оплаты')
plt.xlabel('Метод оплаты')
plt.ylabel('Город')

plt.show()
```
![](https://github.com/Dimonius73/Diplom-DE/blob/main/Изображения/04-тепловая%20карта.png)

Из полученной карты можно сделать вывод о том, что:
  1. Метод оплаты *Cash* чаще всего используют в городе *Naypyitaw*
  2. Метод оплаты *Credit Card* наиболее часто используют в городе *Mandalay*
  3. Метод оплаты *Ewallet* наиболее часто используют в городе *Yangon*

**Общие выводы:**

По результатам анализа были определены основных характеристики, категориальные признаки, а также выполнена базовая проверка данных на ошибки и `Null` значения.

## <a name="second-stage"></a> 3. Формирование состава таблиц, построение ER-диаграмм и DDL-запросов

#### Нормализованная схема данных (NDS)

![](https://github.com/Dimonius73/Diplom-DE/blob/main/Изображения/05-nds.png)

[DDL-запросы](https://github.com/Dimonius73/Diplom-DE/blob/main/ER%20and%20DDL/nds_ddl.sql)

В процессе решения данной задачи был сформирован состав таблиц нормализованной схемы данных. Были исключены исчисляемые атрибуты (`tax_amount`, `total`, `cogs`, `gross_income`).

*Нормализация данных помогает уменьшить избыточность информации и повысить эффективность хранения.*


**Нормализованная схема включает в себя следующие таблицы:**
1. Города (`nds.cities`):
    * Таблица для хранения городов.
2. Филиалы (`nds.branches`):
3. Продуктовые линейки (`nds.product_lines`):
    * Таблица для хранения категорий товаров.
4. Способы оплаты (`nds.payment_methods`):
    * Таблица для хранения доступных методов оплаты.
5. Типы клиентов (`nds.customer_types`):
    * Таблица для хранения различных типов клиентов.
6. Продажи (`nds.sales`):
    * Основная таблица для хранения информации о продажах. Отдельные столбцы для даты, времени, цены, количества и других характеристик продажи.
      Внешние ключи связывают записи в этой таблице с соответствующими записями в таблицах городов, филиалов, категорий товаров, способов оплаты и типов клиентов.

#### Таблицы фактов и измерений  по схеме звезда (DDS)

В процессе решения данной задачи был сформирован состав таблиц фактов и измерений по схеме звезда, предназначенной для анализа данных о покупках.

*Схема звезда (Star Schema) представляет собой тип схемы "факты и измерения", где факты (данные о покупках) хранятся в одной основной таблице, а измерения (характеристики покупки) хранятся в отдельных таблицах, соединенных через внешние ключи. Данные в такой схеме денормализованны для увеличения эффективности при запросах.*

**Диаграмма:**

![](https://github.com/Dimonius73/Diplom-DE/blob/main/Изображения/06-dds.png)

[DDL-запросы](https://github.com/Dimonius73/Diplom-DE/blob/main/ER%20and%20DDL/dds_ddl.sql)

**Схема звезды включает в себя следующие таблицы:**
1. Города (`dds.cities`):
    * Таблица для хранения городов.
2. Филиалы (`dds.branches`):
    * Таблица для хранения информации о филиалах, ссылающаяся на таблицу городов.
3. Продуктовые линейки (`dds.product_lines`):
    * Таблица для хранения категорий товаров.
4. Способы оплаты (`dds.payment_methods`):
    * Таблица для хранения доступных методов оплаты.
5. Типы клиентов (`dds.customer_types`):
    * Таблица для хранения различных типов клиентов.
6. Дата продажи (`dds.sales_dates`):
    * Таблица для хранения информации о датах продажи, включая элементы дат.
7. Время продажи (`dds.sales_time`):
    * Таблица для хранения информации о времени продажи, включая элементы времени.
8. Рейтинг продаж (`dds.sales_rating`):
    * Таблица для хранения рейтингов продаж.
9. Продажи (`dds.sales`):
    * Основная таблица для хранения фактов о продажах. Содержит информацию о продажах, включая дату, время, место, тип клиента, продукт, стоимость, налог и другие характеристики.
    Связана с таблицами измерений через внешние ключи.

#### Структура и типы данных таблицы для хранения исходных данных

**Диаграмма:**
![](https://github.com/Dimonius73/Diplom-DE/blob/main/Изображения/07-unp.png)

[DDL-запросы](https://github.com/Dimonius73/Diplom-DE/blob/main/ER%20and%20DDL/unp_ddl.sql)

В данной таблице изменены названия атрибутов, соединены время и дата (преобразованы в тип данных `DateTime`), а также добавлен столбец `insert_time` - фактическое время на момент insert'а строки в таблицу.

## <a name="third-stage"></a> 4. Разработка ETL-процессов

Для решения этой задачи было решено разделить процессы первичной выгрузки данных из источника и перемещение их в требуемые схемы на 2 DAG.
Все окружение для реализации процессов поднято: [в контейнеризированной среде](https://github.com/Dimonius73/Diplom-DE/blob/main/ETL/docker-compose.yaml)

через PowerShell 

![](https://github.com/Dimonius73/Diplom-DE/blob/main/Изображения/powershell.png)

И через Docker передается в AIRFLOW DAGs

![](https://github.com/Dimonius73/Diplom-DE/blob/main/Изображения/docker.jpg)

**Краткое описание:** DAG выполняет выгрузку данных из источника (через Kaggle API), валидацию (проверка на `Null` значения, дубликаты строк, наличие валидных значений атрибутов, а также соответствие формата `Invoice ID` регулярному выражению) и преобразование типов (раздельные атрибуты даты и времени преобразовываются в один) и загрузку данных в ClickHouse.

**Код Airflow DAG ([data_load_dag.py](https://github.com/Dimonius73/Diplom-DE/blob/main/ETL/data_load_dag.py):)**
```python
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
```

**Граф задач:**

![](https://github.com/Dimonius73/Diplom-DE/blob/main/Изображения/da_load_dag.jpg)

#### Обработка и загрузка данных в NDS и DDS, перемещение исходных данных

**Краткое описание:** DAG выполняет выгрузку данных из источника (таблица исходных данных в ClickHouse), маппинг значений (преобразование в `id`), загрузку данных в NDS и DDS, а также перемещение исходных данных из таблицы `unprocessed_data` в таблицу `processed_data`, очистку таблицы `unprocessed_data`.

**Код Airflow DAG ([data_processing_dag.py](https://github.com/Dimonius73/Diplom-DE/blob/main/ETL/data_processing_dag.py):)**
```python
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
```

**Граф задач:**
![](https://github.com/Dimonius73/Diplom-DE/blob/main/Изображения/data_processing_dag.jpg)

Проверяем загрузку данных:
![](https://github.com/Dimonius73/Diplom-DE/blob/main/Изображения/zagr.png)

![](https://github.com/Dimonius73/Diplom-DE/blob/main/Изображения/zagr2.png)
