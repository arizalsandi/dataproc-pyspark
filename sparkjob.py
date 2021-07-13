from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import date

ADDITIONAL_DAYS = 753
PROJECT_ID = 'blankspace-319301'
BUCKET_NAME = 'etl-spark'
DATASET_ID = 'flight_spark'

#Create Spark Session
spark = SparkSession.builder.getOrCreate()

#Create Schema for BigQuery
schema = StructType([
    StructField("id", IntegerType()),
    StructField("airline_code", StringType()),
    StructField("flight_num", IntegerType()),
    StructField("source_airport", StringType()),
    StructField("destination_airport", StringType()),
    StructField("distance", LongType()),
    StructField("flight_date", DateType()),
    StructField("departure_time", LongType()),
    StructField("arrival_time", LongType()),
    StructField("departure_delay", LongType()),
    StructField("arrival_delay", LongType()),
    StructField("airtime", LongType()),
])


#Extract From GCS to Spark Dataframe
df_flight = spark.read.format("json") \
    .load(f'gs://{BUCKET_NAME}/etl-flight-input/*.json',schema=schema)
df_flight.printSchema()

#Transform Flight_Date 
df_flight = df_flight.withColumn('flight_date', date_add(df_flight.flight_date, ADDITIONAL_DAYS))
df_flight.printSchema()
df_flight.show()

#Partition Spark Dataframe for Bigquery
total_flight = df_flight.groupBy("flight_date", "airline_code") \
                .count() \
                .orderBy("flight_date") \

total_flight.show()

avg_delay = df_flight.groupBy("flight_date","airline_code") \
                .agg(avg("departure_delay").alias("avg_departure_delay"), \
                     avg("arrival_delay").alias("avg_arrival_delay")) \

avg_delay.show()

#Load Spark Dataframe to Bigquery
df_flight.write.mode('overwrite').format('bigquery') \
    .option('temporaryGcsBucket', BUCKET_NAME) \
    .option('partitionField', 'flight_date') \
    .save(f'{PROJECT_ID}:{DATASET_ID}.flights_summary')

total_flight.write.mode('overwrite').format('bigquery') \
    .option('temporaryGcsBucket', BUCKET_NAME) \
    .option('partitionField', 'flight_date') \
    .save(f'{PROJECT_ID}:{DATASET_ID}.count_total_flight')

avg_delay.write.mode('overwrite').format('bigquery') \
    .option('temporaryGcsBucket', BUCKET_NAME) \
    .option('partitionField', 'flight_date') \
    .save(f'{PROJECT_ID}:{DATASET_ID}.count_delay')


#Spark Dataframe to another format
df_flight.write.mode('overwrite') \
    .partitionBy('flight_date') \
    .format('parquet') \
    .save(f'gs://{BUCKET_NAME}/etl-flight-output/output-parquet/flights.parquet')

df_flight.write.mode('overwrite') \
    .partitionBy('flight_date') \
    .format('csv') \
    .save(f'gs://{BUCKET_NAME}/etl-flight-output/output-csv/flights.csv')

spark.stop()