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

[DDL-запросы для создания и заполнения таблиц в PostgreSQL](https://github.com/Dimonius73/Diplom-DE/blob/main/ER%20and%20DDL/nds_ddl.sql)

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
