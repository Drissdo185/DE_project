# DE_project
docker compose -f docker-compose.yml up -d 


docker exec -ti datalake-trino bash


SQL
```
CREATE SCHEMA IF NOT EXISTS mlek2.bronze
WITH (location = 's3://bronze-data/');

-- Create the table within the schema
CREATE TABLE IF NOT EXISTS mlek2.bronze.nyc_taxi (
  congestion_surcharge DOUBLE,
  dolocationid INT,
  dropoff_datetime TIMESTAMP,
  extra DOUBLE,
  fare_amount DOUBLE,
  improvement_surcharge DOUBLE,
  mta_tax DOUBLE,
  passenger_count INT,
  payment_type INT,
  pickup_datetime TIMESTAMP,
  pulocationid INT,
  ratecodeid DOUBLE,
  store_and_fwd_flag VARCHAR(30),
  tip_amount DOUBLE,
  tolls_amount DOUBLE,
  total_amount DOUBLE,
  trip_distance DOUBLE,
  vendorid INT
) WITH (
  external_location = 's3://bronze-data/raw_data',
  format = 'PARQUET'
);

```