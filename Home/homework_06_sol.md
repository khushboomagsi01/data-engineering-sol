# Module 6 Homework - Solutions

## Question 1: Install Spark and PySpark

**What's the output of `spark.version`?**

## Code

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("hw6") \
    .getOrCreate()

print(spark.version)
```

**Answer: `4.1.1`**

## Question 2: Yellow November 2025

Read the November 2025 Yellow data, repartition to 4 partitions, and save as parquet.

## Code

```python
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
df.repartition(4).write.parquet("yellow_tripdata_2025-11_partitioned", mode="overwrite")
```

File sizes:
```
part-00000 — 24M
part-00001 — 24M
part-00002 — 24M
part-00003 — 24M
```

**Answer: `25MB`**


## Question 3: Count records

How many taxi trips were there on the 15th of November?

## Code

```python
from pyspark.sql.functions import col, to_date

nov15_count = df.filter(to_date(col("tpep_pickup_datetime")) == "2025-11-15").count()
print(nov15_count)  # 162604
```

**Answer: `162,604`**


## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

## Code

```python
from pyspark.sql.functions import unix_timestamp

df_with_hours = df.withColumn(
    "trip_hours",
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 3600.0
)
longest = df_with_hours.selectExpr("max(trip_hours) as max_hours").collect()[0]["max_hours"]
print(f"{longest:.1f}")  # 90.6
```

**Answer: `90.6` hours**


## Question 5: User Interface

Spark's User Interface which shows the application's dashboard runs on which local port?

**Answer: `4040`**


## Question 6: Least frequent pickup location zone

## Code

```python
zones = spark.read.option("header", "true").csv("taxi_zone_lookup.csv")
zones.createOrReplaceTempView("zones")
df.createOrReplaceTempView("trips")

result = spark.sql(
    "SELECT z.Zone, COUNT(*) as cnt "
    "FROM trips t "
    "JOIN zones z ON t.PULocationID = z.LocationID "
    "GROUP BY z.Zone "
    "ORDER BY cnt ASC "
    "LIMIT 5"
)
result.show(truncate=False)
```

```
+---------------------------------------------+---+
|Zone                                         |cnt|
+---------------------------------------------+---+
|Governor's Island/Ellis Island/Liberty Island|1  |
|Eltingville/Annadale/Prince's Bay            |1  |
|Arden Heights                                |1  |
|Port Richmond                                |3  |
|Rikers Island                                |4  |
+---------------------------------------------+---+
```

**Answer: `Governor's Island/Ellis Island/Liberty Island`**
