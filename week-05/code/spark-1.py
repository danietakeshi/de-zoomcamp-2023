import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

schema = types.StructType ([
    types.StructField('hvfhs_license_num', types.StringType(), True),
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.LongType(), True),
    types.StructField('DOLocationID', types.LongType(), True),
    types.StructField('SR_Flag', types.StringType(), True),
    ])

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .parquet('fhvhv_tripdata_2021-01.parquet')

df = df.repartition(24)

df.write.parquet('fhvhv/2021/01/')

df.show()
