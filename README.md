# DE_project
docker compose -f docker-compose.yml up -d 


docker exec -ti datalake-trino bash


SQL
```
CREATE SCHEMA IF NOT EXISTS mlek2.bronze
WITH (location = 's3://bronze-data/');

-- Create the table within the schema
CREATE TABLE IF NOT EXISTS mlek2.bronze.banking_data (
  age INT,
  job VARCHAR(30),
  marital VARCHAR(10),
  education VARCHAR(10),
  default VARCHAR(3),
  balance INT,
  housing VARCHAR(3),
  loan VARCHAR(3),
  contact VARCHAR(10),
  day INT,
  month VARCHAR(3),
  duration INT,
  campaign INT,
  pdays INT,
  previous INT,
  poutcome VARCHAR(10)
) WITH (
  external_location = 's3://bronze-data/raw_data',
  format = 'PARQUET'
);


```