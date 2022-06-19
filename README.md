# covid-19_ETL process

### Project Summary
This project extracts covid data provided by Our World in Data and creates cloud-data-lake on Google Cloud Platform.
I want to build data-warehouse to find important information about covid-19.
I believe that this project will help someone who want to build data warehouse about covid-19.


### Data Source

[Our World in Data](https://github.com/owid/covid-19-data)

### Architecture

Data are uploading to Google Cloud Storage bucket. GCS will act as the data lake where all raw files are stored.
And then Data will be loaded as data-warehouse on BigQuery. The ETL process will take data from data-warehouse and create data mart tables using by data-warehouse.

![DAGS!](https://user-images.githubusercontent.com/92921909/174450262-fb32904b-f548-4900-80dd-0adcc414b78a.png)


Here are the tech stack that i used here:

* Google Cloud Storage
* Google BigQuery
* Apache Airflow

### Project Instructions

[Visit here](https://velog.io/@hyunwoozz/airflow-Covid-19-ETL-by-bigquery-1) I wrote about how i install all infrastructure.
