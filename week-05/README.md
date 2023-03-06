## Week 5 Homework 

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the FHVHV 2021-06 data found here. [FHVHV Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz )


### Question 1: 

**Install Spark and PySpark** 

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?
- [x] 3.3.2
- [ ] 2.1.4
- [ ] 1.2.3
- [ ] 5.4
</br></br>

```python
spark.version
'3.3.2'
```

### Question 2: 

**HVFHW June 2021**

Read it with Spark using the same schema as we did in the lessons.</br> 
We will use this dataset for all the remaining questions.</br>
Repartition it to 12 partitions and save it to parquet.</br>
What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.</br>


- [ ] 2MB
- [x] 24MB
- [ ] 100MB
- [ ] 250MB
</br></br>


### Question 3: 

**Count records**  

How many taxi trips were there on June 15?</br></br>
Consider only trips that started on June 15.</br>

- [ ] 308,164
- [ ] 12,856
- [x] 452,470
- [ ] 50,982
</br></br>

```python
from pyspark.sql import functions as F

df = df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime))

df.select('pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
  .filter(df.pickup_date == '2021-06-15').count()
```

### Question 4: 

**Longest trip for each day**  

Now calculate the duration for each trip.</br>
How long was the longest trip in Hours?</br>

- [x] 66.87 Hours
- [ ] 243.44 Hours
- [ ] 7.68 Hours
- [ ] 3.32 Hours
</br></br>

```python
df_fhvhv = spark.read.parquet('fhvhv/2021/06/*')

df_fhvhv.createOrReplaceTempView('fhvhv_data')

spark.sql("""
SELECT
    *,
    (unix_timestamp(dropoff_datetime)-unix_timestamp(pickup_datetime))/(60)/(60) as diff
FROM
    fhvhv_data
ORDER BY
    (unix_timestamp(dropoff_datetime)-unix_timestamp(pickup_datetime))/(60)/(60) DESC
LIMIT 1
""").show()
```

### Question 5: 

**User Interface**

 Spark’s User Interface which shows application's dashboard runs on which local port?</br>

- [ ] 80
- [ ] 443
- [x] 4040
- [ ] 8080
</br></br>


### Question 6: 

**Most frequent pickup location zone**

Load the zone lookup data into a temp view in Spark</br>
[Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)</br>

Using the zone lookup data and the fhvhv June 2021 data, what is the name of the most frequent pickup location zone?</br>

- [ ] East Chelsea
- [ ] Astoria
- [ ] Union Sq
- [x] Crown Heights North
</br></br>

```python
schema = types.StructType ([
    types.StructField('LocationID', types.LongType(), True),
    types.StructField('Borough', types.StringType(), True),
    types.StructField('Zone', types.StringType(), True),
    types.StructField('service_zone', types.StringType(), True)
    ])

df_zone = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('taxi_zone_lookup.csv')

df_zone.createOrReplaceTempView('zone_data')

spark.sql("""
SELECT
    pu.Zone,
    COUNT(1) count_trips
FROM
    fhvhv_data fd
LEFT JOIN
    zone_data pu ON fd.PULocationID = pu.LocationID
LEFT JOIN 
    zone_data do ON fd.DOLocationID = do.LocationID
GROUP BY
    pu.Zone
ORDER BY
    COUNT(1) DESC
LIMIT 5
""").show()
```


## Submitting the solutions

* Form for submitting: https://forms.gle/EcSvDs6vp64gcGuD8
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 06 March (Monday), 22:00 CET


## Solution

We will publish the solution here
