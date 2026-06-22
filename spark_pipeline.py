import sys
import os
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession, functions as F
from datetime import datetime, timedelta


input_path  = sys.argv[1]
output_path = sys.argv[2]

input_path_split = input_path.replace('s3://', '').split('/')
bucket, key = input_path_split[0], input_path_split[1]

spark = SparkSession.builder \
    .appName('Premier League Data Analysis') \
    .getOrCreate()

today = datetime.today()
today_str = today.strftime('%Y-%m-%d')
last_week_str = (today - timedelta(days=7)).strftime('%Y-%m-%d')


s3 = boto3.client('s3')

def s3_path_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False

df_today = spark.read.csv(
    f'{input_path}/date={today_str}/standings.csv',
    header=True,
    inferSchema=True,
    sep=",",
    quote='"',
    escape='"',
    multiLine=True,
    mode="PERMISSIVE"
)

df_today = df_today \
    .withColumnRenamed('points', 'points_today') \
    .withColumnRenamed('position', 'position_today') \
    .select('position_today', 'name', 'points_today')

if s3_path_exists(bucket, key=f'{key}/date={last_week_str}/standings.csv'):
    df_previous = spark.read.csv(
        f'{input_path}/date={last_week_str}/standings.csv',
        header=True,
        inferSchema=True,
        sep=",",
        quote='"',
        escape='"',
        multiLine=True,
        mode="PERMISSIVE"
    )

    df_previous = df_previous \
        .withColumnRenamed('points', 'points_last_week') \
        .withColumnRenamed('position', 'position_last_week') \
        .select('position_last_week', 'name', 'points_last_week')

    df_joined = df_today.join(df_previous, on='name')

    df_joined = df_joined \
        .withColumn(
            'points_gained',
            F.col('points_today') - F.col('points_last_week') 
        ) \
        .withColumn(
            'positions_gained',
            F.col('position_last_week') - F.col('position_today')
        )
    
    df_joined = df_joined \
        .select(
            F.col('position_today').alias('position'),
            'positions_gained',
            'name',
            F.col('points_today').alias('points'),
            'points_gained'
        )

    df_result = df_joined

else:
    df_result = df_today

df_result.write \
    .mode('overwrite') \
    .parquet(f'{output_path}/date={today_str}')