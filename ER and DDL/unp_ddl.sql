CREATE TABLE default.processed_data
(
    id String,
    datetime DateTime,
    city String,
    branch String,
    customer_type String,
    gender String,
    product_line String,
    unit_price Float64,
    quantity UInt32,
    tax Float64,
    total Float64,
    payment_method String,
    cogs Float64,
    gross_margin_percentage Float64,
    gross_income Float64,
    rating Float64,
    insert_time DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY datetime
PRIMARY KEY tuple(id)
ORDER BY (id,
          datetime)
SETTINGS index_granularity = 8192;

CREATE TABLE default.unprocessed_data
(
    id String,
    datetime DateTime,
    city String,
    branch String,
    customer_type String,
    gender String,
    product_line String,
    unit_price Float64,
    quantity UInt32,
    tax Float64,
    total Float64,
    payment_method String,
    cogs Float64,
    gross_margin_percentage Float64,
    gross_income Float64,
    rating Float64,
    insert_time DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY datetime
PRIMARY KEY tuple(id)
ORDER BY (id,
          datetime)
SETTINGS index_granularity = 8192;