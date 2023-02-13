-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `coherent-bliss-275820.dezoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://zoomcamp-bucket/yellow_tripdata_2019-*.csv.gz', 'gs://zoomcamp-bucket/yellow_tripdata_2020-*.csv.gz']
);


select * from `coherent-bliss-275820.dezoomcamp.external_yellow_tripdata` LIMIT 10;


-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `coherent-bliss-275820.dezoomcamp.yellow_tripdata`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `coherent-bliss-275820.dezoomcamp.external_yellow_tripdata`;

select * from `coherent-bliss-275820.dezoomcamp.yellow_tripdata` LIMIT 10;

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `coherent-bliss-275820.dezoomcamp.external_green_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://zoomcamp-bucket/green_tripdata_2019-*.csv.gz', 'gs://zoomcamp-bucket/green_tripdata_2020-*.csv.gz']
);


select * from `coherent-bliss-275820.dezoomcamp.external_green_tripdata` LIMIT 10;


-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `coherent-bliss-275820.dezoomcamp.green_tripdata`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `coherent-bliss-275820.dezoomcamp.external_green_tripdata`;

select * from `coherent-bliss-275820.dezoomcamp.green_tripdata` LIMIT 10;