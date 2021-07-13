#!/bin/bash

#VARIABLES PARAMETER
TEMPLATE = pyspark-flow
REGION = us-central1
ZONE = us-central1-a
PROJECT_ID = blankspace-319301
CLUSTER_NAME = pyspark-gcs-to-bq
BUCKET_NAME = etl-spark
LOCAL_SOURCE = /home/sandi/Desktop/project_blankspace/week3/dataset/*.json

#RENAME LOCAL FILES AND STORE THEM INTO GCS
for files in LOCAL_SOURCE
do
    EXTRACT_FILE = $(echo $files    | grep -Eo '[0-9]{4}-[0-9]{2}-[0-9]{2}')
    TO_DATE = $(date -d "EXTRACT_FILE + 723 days" '+%Y-%m-%d')
    gsutil cp ${files} gs://{BUCKET_NAME}/transformed_date/${TO_DATE}.json
done

#SET PROJECT ID
gcloud config set project_blankspace ${PROJECT_ID}

#CREATE DATAPROC TEMPLATE
gcloud dataproc workflow-templates create ${TEMPLATE}\
    --region=${REGION}

#WORKFLOW TEMPLATE DATAPROC
gcloud beta dataproc workflow-templates set-managed-cluster ${TEMPLATE} \
    --region=${REGION} \
    --bucket=${BUCKET_NAME} \
    --zone=${ZONE} \
    --cluster-name=${CLUSTER_NAME} \
    --single-node \
    --master-machine-type=n1-standard-2 \
    --image-version=1.5-ubuntu18

#ADD JOB PYSPARK TO WORKFLOW-TEMPLATE
gcloud beta dataproc workflow-templates add-job pyspark gs://${BUCKET_NAME}/spark-job/sparkjob.py \
    --step-id="week3-flight-etl" \
    --workflow-templates=${TEMPLATE} \
    --region=${REGION} \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar  

gcloud dataproc workflow-templates instantiate ${TEMPLATE} \
    --region=${REGION}