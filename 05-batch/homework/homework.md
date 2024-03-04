## Week 5 Homework 

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the FHV 2019-10 data found here. [FHV Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz)

### Question 1: 

**Install Spark and PySpark** 

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?


> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)

**- 3.5.0**

```bash
set -e

TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

for MONTH in {1..12}; do
  FMONTH=`printf "%02d" ${MONTH}`

  URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"

  LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  echo "downloading ${URL} to ${LOCAL_PATH}"
  mkdir -p ${LOCAL_PREFIX}
  wget ${URL} -O ${LOCAL_PATH}

done
```

```linux
./download_data.sh fhv 2019
```

```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

spark.version

```

### Question 2: 

**FHV October 2019**

Read the October 2019 FHV into a Spark Dataframe with a schema as we did in the lessons.

Repartition the Dataframe to 6 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 1MB
- 6MB
- 25MB
- 87MB

**- 6.4MB** ~ **- 6MB**

```python
schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True), 
    types.StructField('Affiliated_base_number', types.StringType(), True)
])

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('./data/raw/fhv/2019/10/fhv_tripdata_2019_10.csv.gz', )

df = df.repartition(6)

df.write.parquet('data/pq/fhv/2019/10/',)

#File size:
!ls -lh ./data/pq/fhv/2019/10/*
```

### Question 3: 

**Count records** 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 108,164
- 12,856
- 452,470
- 62,610


> [!IMPORTANT]
> Be aware of columns order when defining schema

**-- 62,610**

```python
from pyspark.sql import functions as F

df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .filter("pickup_date = '2019-10-15'") \
    .count()

#answer 62610
```

or 
```python
df.registerTempTable('fhvhv_2019_10')

spark.sql("""
SELECT
    COUNT(1)
FROM 
    fhvhv_2019_10
WHERE
    to_date(pickup_datetime) = '2019-10-15';
""").show()

#answer 62610
```


### Question 4: 

**Longest trip for each day** 

What is the length of the longest trip in the dataset in hours?

- 631,152.50 Hours
- 243.44 Hours
- 7.68 Hours
- 3.32 Hours

**- 631,152.50 Hours**

```python
df = spark.read.parquet('data/pq/fhv/2019/10/')
df.columns

df \
    .withColumn('duration', (df.dropoff_datetime.cast('long') - df.pickup_datetime.cast('long'))/3600) \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .groupBy('pickup_date') \
        .max('duration') \
    .orderBy('max(duration)', ascending=False) \
    .limit(5) \
    .show()
```
or
```python
spark.sql("""
SELECT
    to_date(pickup_datetime) AS pickup_date,
    MAX((CAST(dropoff_datetime AS LONG) - CAST(pickup_datetime AS LONG)) / 3600) AS duration
FROM 
    fhvhv_2019_10
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT 10;
""").show()
```

### Question 5: 

**User Interface**

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

**-4040**

### Question 6: 

**Least frequent pickup location zone**

Load the zone lookup data into a temp view in Spark</br>
[Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)

Using the zone lookup data and the FHV October 2019 data, what is the name of the LEAST frequent pickup location Zone?</br>

- East Chelsea
- Jamaica Bay
- Union Sq
- Crown Heights North

**-- Jamaica Bay**

```python
df = spark.read.parquet('data/pq/fhv/2019/10/')
df_zones = spark.read.parquet('zones')

df_zones.registerTempTable('zones')
df.registerTempTable('fhvhv_2019_10')
spark.sql("""
SELECT
    pul.Zone,
    COUNT(1)
FROM 
    fhvhv_2019_10 fhv LEFT JOIN zones pul ON fhv.PULocationID = pul.LocationID
                      LEFT JOIN zones dol ON fhv.DOLocationID = dol.LocationID
GROUP BY 
    1
ORDER BY
    2 ASC
LIMIT 5;
""").show()

"""
+--------------------+--------+
|                Zone|count(1)|
+--------------------+--------+
|         Jamaica Bay|       1|
|Governor's Island...|       2|
| Green-Wood Cemetery|       5|
|       Broad Channel|       8|
|     Highbridge Park|      14|
+--------------------+--------+
"""

```

## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw5
- Deadline: See the website
