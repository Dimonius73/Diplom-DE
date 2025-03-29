CREATE SCHEMA IF NOT EXISTS nds;

CREATE TABLE IF NOT EXISTS nds.cities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS nds.branches (
    id SERIAL PRIMARY KEY,
    name VARCHAR(64),
    city_id INT,
    CONSTRAINT fk_city FOREIGN KEY (city_id) REFERENCES nds.cities(id)
);

CREATE TABLE IF NOT EXISTS nds.product_lines (
    id SERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS nds.payment_methods (
    id SERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS nds.customer_types (
    id SERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS nds.sales (
    id VARCHAR(11) PRIMARY KEY,
    datetime TIMESTAMP,
    city_id INTEGER,
    branch_id INTEGER,
    customer_type_id INTEGER,
    gender VARCHAR(8),
    payment_method_id INTEGER,
    product_line_id INTEGER,
    unit_price FLOAT,
    quantity INTEGER,
    tax_percentage FLOAT DEFAULT 5.0,
    gross_margin_percentage FLOAT DEFAULT 4.761905,
    rating FLOAT,
    CONSTRAINT fk_city FOREIGN KEY (city_id) REFERENCES nds.cities(id),
    CONSTRAINT fk_branch FOREIGN KEY (branch_id) REFERENCES nds.branches(id),
    CONSTRAINT fk_customer_type FOREIGN KEY (customer_type_id) REFERENCES nds.customer_types(id),
    CONSTRAINT fk_payment_method FOREIGN KEY (payment_method_id) REFERENCES nds.payment_methods(id),
    CONSTRAINT fk_product_line FOREIGN KEY (product_line_id) REFERENCES nds.product_lines(id)
);

--заполняем таблицы

INSERT INTO nds.cities(name) VALUES
('Yangon'),
('Mandalay'),
('Naypyitaw');

INSERT INTO nds.branches(name, city_id) VALUES
('A', 1),
('B', 2),
('C', 3);

INSERT INTO nds.product_lines(name) VALUES
('Sports and travel'),
('Food and beverages'),
('Health and beauty'),
('Fashion accessories'),
('Electronic accessories'),
('Home and lifestyle');

INSERT INTO nds.customer_types(name) VALUES
('Normal'),
('Member');

INSERT INTO nds.payment_methods(name) VALUES
('Cash'),
('Credit card'),
('Ewallet');
