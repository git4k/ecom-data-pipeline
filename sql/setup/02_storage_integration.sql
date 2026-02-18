CREATE OR REPLACE STORAGE INTEGRATION s3_ecom_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::650251724071:role/SnowflakeS3Role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ecom-curated-zone-kishore/');

DESC INTEGRATION s3_ecom_integration;
