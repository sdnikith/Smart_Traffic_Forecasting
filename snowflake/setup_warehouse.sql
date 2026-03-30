-- Create warehouses with auto-suspend for cost optimization
CREATE WAREHOUSE IF NOT EXISTS TRAFFIC_LOAD_WH
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 3
  SCALING_POLICY = 'STANDARD'
  COMMENT = 'For ETL loading from S3 Delta Lake';

CREATE WAREHOUSE IF NOT EXISTS TRAFFIC_TRANSFORM_WH
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 2
  SCALING_POLICY = 'STANDARD'
  COMMENT = 'For DBT transformations';

CREATE WAREHOUSE IF NOT EXISTS TRAFFIC_ANALYTICS_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 1
  SCALING_POLICY = 'STANDARD'
  COMMENT = 'For Tableau and ad-hoc queries';

CREATE WAREHOUSE IF NOT EXISTS TRAFFIC_ML_WH
  WAREHOUSE_SIZE = 'LARGE'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 2
  SCALING_POLICY = 'STANDARD'
  COMMENT = 'For machine learning model training';

-- Create database and schemas
CREATE DATABASE IF NOT EXISTS TRAFFIC_DB;

CREATE SCHEMA IF NOT EXISTS TRAFFIC_DB.RAW;
CREATE SCHEMA IF NOT EXISTS TRAFFIC_DB.STAGING;
CREATE SCHEMA IF NOT EXISTS TRAFFIC_DB.ANALYTICS;
CREATE SCHEMA IF NOT EXISTS TRAFFIC_DB.ML_OUTPUT;
CREATE SCHEMA IF NOT EXISTS TRAFFIC_DB.EXTERNAL;

-- Create external stage pointing to S3 Delta Lake
CREATE OR REPLACE STAGE TRAFFIC_DB.RAW.S3_TRAFFIC_STAGE
  URL = 's3://traffic-platform-lake/bronze/traffic_events/'
  STORAGE_INTEGRATION = traffic_s3_integration
  FILE_FORMAT = (TYPE = 'PARQUET')
  COMMENT = 'External stage for Delta Lake traffic data';

-- Create external stage for weather data
CREATE OR REPLACE STAGE TRAFFIC_DB.RAW.S3_WEATHER_STAGE
  URL = 's3://traffic-platform-lake/bronze/weather_updates/'
  STORAGE_INTEGRATION = traffic_s3_integration
  FILE_FORMAT = (TYPE = 'JSON')
  COMMENT = 'External stage for weather data';

-- Create external stage for holidays data
CREATE OR REPLACE STAGE TRAFFIC_DB.RAW.S3_HOLIDAYS_STAGE
  URL = 's3://traffic-platform-lake/bronze/holidays/'
  STORAGE_INTEGRATION = traffic_s3_integration
  FILE_FORMAT = (TYPE = 'JSON')
  COMMENT = 'External stage for holidays data';

-- Create external stage for sensor metadata
CREATE OR REPLACE STAGE TRAFFIC_DB.RAW.S3_SENSORS_STAGE
  URL = 's3://traffic-platform-lake/bronze/sensor_metadata/'
  STORAGE_INTEGRATION = traffic_s3_integration
  FILE_FORMAT = (TYPE = 'JSON')
  COMMENT = 'External stage for sensor metadata';

-- Create tables from external stages
CREATE OR REPLACE TABLE TRAFFIC_DB.RAW.TRAFFIC_EVENTS (
    sensor_id VARCHAR(50),
    timestamp TIMESTAMP_NTZ,
    traffic_volume INTEGER,
    speed_mph FLOAT,
    occupancy_pct FLOAT,
    road_name VARCHAR(100),
    direction VARCHAR(10),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    year INTEGER,
    month INTEGER,
    day INTEGER,
    etl_loaded_at TIMESTAMP_NTZ
)
  COMMENT = 'Raw traffic sensor events from Delta Lake'
  CLUSTER BY (sensor_id, timestamp);

CREATE OR REPLACE TABLE TRAFFIC_DB.RAW.WEATHER_UPDATES (
    timestamp TIMESTAMP_NTZ,
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    temp_fahrenheit FLOAT,
    humidity_pct FLOAT,
    wind_speed_mph FLOAT,
    weather_main VARCHAR(50),
    weather_description VARCHAR(100),
    visibility_miles FLOAT,
    clouds_pct INTEGER,
    rain_1h_mm FLOAT,
    snow_1h_mm FLOAT,
    etl_loaded_at TIMESTAMP_NTZ
)
  COMMENT = 'Raw weather updates from API'
  CLUSTER BY (timestamp);

CREATE OR REPLACE TABLE TRAFFIC_DB.RAW.HOLIDAYS (
    date DATE,
    name VARCHAR(100),
    etl_loaded_at TIMESTAMP_NTZ
)
  COMMENT = 'US federal holidays'
  CLUSTER BY (date);

CREATE OR REPLACE TABLE TRAFFIC_DB.RAW.SENSOR_METADATA (
    sensor_id VARCHAR(50),
    road_name VARCHAR(100),
    city VARCHAR(50),
    direction VARCHAR(10),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    is_active BOOLEAN,
    installed_date TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    valid_from TIMESTAMP_NTZ,
    valid_to TIMESTAMP_NTZ,
    is_current BOOLEAN
)
  COMMENT = 'Sensor metadata with SCD Type 2'
  CLUSTER BY (sensor_id, valid_from);

-- Grant usage on database and schemas
GRANT USAGE ON DATABASE TRAFFIC_DB TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA TRAFFIC_DB.RAW TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA TRAFFIC_DB.STAGING TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA TRAFFIC_DB.ANALYTICS TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA TRAFFIC_DB.ML_OUTPUT TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA TRAFFIC_DB.EXTERNAL TO ROLE SYSADMIN;

-- Log setup completion
SELECT 'Snowflake warehouse setup completed successfully' AS status;
