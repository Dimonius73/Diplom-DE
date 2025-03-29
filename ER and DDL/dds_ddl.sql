CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.cities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS dds.branches (
    id SERIAL PRIMARY KEY,
    name VARCHAR(64),
    city VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS dds.product_lines (
    id SERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS dds.payment_methods (
    id SERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS dds.customer_types (
    id SERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS dds.sales_dates (
    id SERIAL PRIMARY KEY,
    date DATE,
    year INTEGER,
    month INTEGER,
    month_name VARCHAR(9),
    day INTEGER,
    day_of_the_week VARCHAR(9)
);

CREATE TABLE IF NOT EXISTS dds.sales_time (
    id SERIAL PRIMARY KEY,
    datetime timestamp,
    hour INTEGER,
    minute INTEGER,
    second INTEGER
);

CREATE TABLE IF NOT EXISTS dds.sales_rating (
    id SERIAL PRIMARY KEY,
    rating FLOAT
);

CREATE TABLE IF NOT EXISTS dds.sales (
    id VARCHAR(11) PRIMARY KEY,
    date_id INTEGER,
    time_id INTEGER,
    city_id INTEGER,
    branch_id INTEGER,
    customer_type_id INTEGER,
    gender VARCHAR(8),
    payment_method_id INTEGER,
    product_line_id INTEGER,
    rating_id INTEGER,
    unit_price FLOAT,
    quantity INTEGER,
    tax_amount FLOAT,
    total FLOAT,
    cogs FLOAT,
    gross_income FLOAT,
    CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES dds.sales_dates(id),
    CONSTRAINT fk_time FOREIGN KEY (time_id) REFERENCES dds.sales_time(id),
    CONSTRAINT fk_city FOREIGN KEY (city_id) REFERENCES dds.cities(id),
    CONSTRAINT fk_branch FOREIGN KEY (branch_id) REFERENCES dds.branches(id),
    CONSTRAINT fk_customer_type FOREIGN KEY (customer_type_id) REFERENCES dds.customer_types(id),
    CONSTRAINT fk_payment_method FOREIGN KEY (payment_method_id) REFERENCES dds.payment_methods(id),
    CONSTRAINT fk_product_line FOREIGN KEY (product_line_id) REFERENCES dds.product_lines(id),
    CONSTRAINT fk_rating FOREIGN KEY (rating_id) REFERENCES dds.sales_rating(id)
);

--заполняем таблицы

INSERT INTO dds.cities(name) VALUES
('Yangon'),
('Mandalay'),
('Naypyitaw');

INSERT INTO dds.branches(name, city) VALUES
('A', 'Yangon'),
('B', 'Mandalay'),
('C', 'Naypyitaw');

INSERT INTO dds.product_lines(name) VALUES
('Sports and travel'),
('Food and beverages'),
('Health and beauty'),
('Fashion accessories'),
('Electronic accessories'),
('Home and lifestyle');

INSERT INTO dds.customer_types(name) VALUES
('Normal'),
('Member');

INSERT INTO dds.payment_methods(name) VALUES
('Cash'),
('Credit card'),
('Ewallet');
