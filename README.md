# Dataproc-Pyspark

Perform ETL process using Dataproc as clusters manager to perform Spark Job using Pyspark and stored the results into Bigquery and Google Cloud Storage

# Data Sources
1. Json as our Dataset

And we need to transformed data into :
1. CSV
2. Parquet
3. Big Query Table

# Prerequisite
1. Python 3.6.9 ( or more )
2. Google Cloud Platform
  - Dataproc
  - Google Cloud Storage
  - Google Big Query

# Setup
1. You need to enable Dataproc API, to create some Cluster
2. After you enabled the API, you have two ways to perform the Job.

## By GUI
1. Move to Dataproc field
2. Create your cluster by press Create Cluster
3. Complete your clusters configuration
4. After you finished with your cluster, make sure your cluster have a "Running" status
5. Inside your cluster dashboard, choose Submit Job to perform your pyspark.py and here some configuration that i used

![submitjobmanual](https://user-images.githubusercontent.com/84316622/125414552-b82893e1-5c29-4f24-b1e6-14dcae1cebe7.png)

6. If your job was finished and running smoothly, you can see green mark on your job's name

![succeededjob](https://user-images.githubusercontent.com/84316622/125414944-cf981212-8e4e-401c-8670-6a688d7ce8f9.png)


## By Shell Script
1. Install Google Cloud SDK by referring this site : https://cloud.google.com/sdk/docs/quickstart
2. Set up your workflow-template by using this command
```
gcloud dataproc workflow-templates create ${TEMPLATE}\
    --region=${REGION}
```
3. Set your clusters manager to perform spark job 
```
gcloud beta dataproc workflow-templates set-managed-cluster ${TEMPLATE} \
    --region=${REGION} \
    --bucket=${BUCKET_NAME} \
    --zone=${ZONE} \
    --cluster-name=${CLUSTER_NAME} \
    --single-node \
    --master-machine-type=n1-standard-2 \
    --image-version=1.5-ubuntu18
```

4. Add your job into your workflow-template
```
gcloud beta dataproc workflow-templates add-job pyspark gs://${BUCKET_NAME}/spark-job/sparkjob.py \
    --step-id="week3-flight-etl" \
    --workflow-templates=${TEMPLATE} \
    --region=${REGION} \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar  
```
5. Run your workflow-template bu using this code
```
gcloud dataproc workflow-templates instantiate ${TEMPLATE} \
    --region=${REGION}
```

# Output
With completion your pyspark job, you will get several output

## Google Cloud Storage
CSV

1. Check on your Google Cloud Storage, there will be a partitioned CSV files by Flight Date

![csvoutput](https://user-images.githubusercontent.com/84316622/125418553-fd267e20-7027-45da-a5b0-d282454a9992.png)

Parquet

2. Check on your Google Cloud Storage, there will be a partitioned Parquet files by Flight Date
![parquetoutput](https://user-images.githubusercontent.com/84316622/125418857-27b6361a-de33-4c00-b7eb-a44b80396f54.png)

### Comparation
I will take 1 file from one of the Flight Date as a comparation between Json, CSV and Parquet

Json File

![jsonsize](https://user-images.githubusercontent.com/84316622/125419424-d235cde1-137b-4a25-8534-ddb4a6b53514.png)

CSV

![csvsize](https://user-images.githubusercontent.com/84316622/125419460-163fd6a3-7f84-45e7-b924-56f93ce899b0.png)

Parquet

![parquetsize](https://user-images.githubusercontent.com/84316622/125419498-8e047ae0-fe44-4112-b63d-17da7e9ff533.png)

As a Data Engineer, beside building pipeline we need to pay attention to reduce Cost effectively.

## Big Query
1. Flights Summary

This table have a summary from all the raw data but have transformed Flight Date and partitioned by Flight Date

![bq_flights_summary](https://user-images.githubusercontent.com/84316622/125420694-e39342d1-ea81-43a5-a99e-d4918fe974c4.png)

2. Count Total Flight

This table have an aggregated data to count Airline Code and partitioned by Flight Date

![bq_count_total_flight](https://user-images.githubusercontent.com/84316622/125421063-33681b2e-91f4-4779-badf-2bbeb27150bd.png)

3. Count Delay

This table have an aggregated data to count average delay by Departure and Arrived Delay

![bq_count_delay](https://user-images.githubusercontent.com/84316622/125421242-21980bd8-e5b9-4fd0-8a1d-6adf9a46f9b6.png)

