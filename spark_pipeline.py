import sys
from pyspark.sql import SparkSession, functions as F
from datetime import datetime

spark = SparkSession.builder \
    .appName('Premier League Data Analysis') \
    .getOrCreate()

input_path  = sys.argv[1]
output_path = sys.argv[2]

df = spark.read.csv(
    input_path,
    header=True,
    inferSchema=True,
    sep=",",
    quote='"',
    escape='"',
    multiLine=True,
    mode="PERMISSIVE"
)

df = df \
    .withColumn(
        'points_per_game',
        F.round(
            F.col('points') / F.col('playedGames'), 2
        )
    ) \
    .select(
        'position', 'name', 'won', 'lost',
        'draw', 'points', 'points_per_game'
    )

dt = datetime.today().strftime('%Y-%m-%d')

df.write \
    .mode('overwrite') \
    .parquet(f'{output_path}/date={dt}/')