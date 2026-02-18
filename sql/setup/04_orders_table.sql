CREATE OR REPLACE TABLE orders (
  category        STRING,
  cost_price      FLOAT,
  country         STRING,
  customer_id     NUMBER,
  customer_name   STRING,
  email           STRING,
  order_id        STRING,
  order_timestamp TIMESTAMP_NTZ,
  product_id      NUMBER,
  product_name    STRING,
  profit          FLOAT,
  quantity        NUMBER,
  revenue         FLOAT,
  status          STRING,
  unit_price      FLOAT
);

COPY INTO orders
FROM (
  SELECT
    $1:category::STRING,
    $1:cost_price::FLOAT,
    $1:country::STRING,
    $1:customer_id::NUMBER,
    $1:customer_name::STRING,
    $1:email::STRING,
    $1:order_id::STRING,
    $1:order_timestamp::TIMESTAMP_NTZ,
    $1:product_id::NUMBER,
    $1:product_name::STRING,
    $1:profit::FLOAT,
    $1:quantity::NUMBER,
    $1:revenue::FLOAT,
    $1:status::STRING,
    $1:unit_price::FLOAT
  FROM @ecom_s3_stage/orders/
)
FILE_FORMAT = (FORMAT_NAME = 'ecom_parquet_format')
ON_ERROR = 'CONTINUE';
