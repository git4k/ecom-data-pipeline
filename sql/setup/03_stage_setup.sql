CREATE OR REPLACE FILE FORMAT ecom_parquet_format
  TYPE = 'PARQUET'
  SNAPPY_COMPRESSION = TRUE;

CREATE OR REPLACE STAGE ecom_s3_stage
  STORAGE_INTEGRATION = s3_ecom_integration
  URL = 's3://ecom-curated-zone-kishore/'
  FILE_FORMAT = ecom_parquet_format;

LIST @ecom_s3_stage;
