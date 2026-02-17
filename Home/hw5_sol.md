# Homework 5 Solution: Batch Processing with PySpark

**Dataset:** [Yellow Taxi Trip Data - October 2024](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet)

**Code:** [`hw05_sol.py`](hw05_sol.py)

---

## Question 1: Install Spark and PySpark

**What's the output of `spark.version`?**

```
spark.version = 4.1.1
```

**Answer: `4.1.1`**

---

## Question 2: Yellow October 2024 — Parquet File Size

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB
- 75MB
- 100MB

After repartitioning to 4 partitions and saving as parquet:

```
part-00000.snappy.parquet: 22.4 MB
part-00001.snappy.parquet: 22.4 MB
part-00002.snappy.parquet: 22.4 MB
part-00003.snappy.parquet: 22.4 MB
Average size: 22.4 MB
```

**Answer: 25MB** (closest match)

---

## Question 3: Count Records — Trips on October 15
How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 85,567
- 105,567
- 125,567
- 145,567

**sql code**

```sql
SELECT COUNT(*) AS trip_count
FROM yellow
WHERE CAST(tpep_pickup_datetime AS DATE) = '2024-10-15'
```

Result: **128,893**

**Answer: 125,567** (closest match)

---

## Question 4: Longest Trip in Hours

What is the length of the longest trip in the dataset in hours?

- 122
- 142
- 162
- 182

**sql code**

```sql
SELECT
    ROUND(
        (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600.0,
        2
    ) AS trip_hours
FROM yellow
ORDER BY trip_hours DESC
LIMIT 1
```

**Answer: 162**

---

## Question 5: Spark User Interface Port

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

Spark's Web UI (application dashboard) runs on port **4040** by default.

**Answer: 4040**

---

## Question 6: Least Frequent Pickup Location Zone

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay

**sql code**

```sql
SELECT z.Zone, COUNT(*) AS trip_count
FROM yellow y
JOIN zones z ON y.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY trip_count ASC
LIMIT 5
```

| Zone | trip_count |
|------|-----------|
| Governor's Island/Ellis Island/Liberty Island | 1 |
| Rikers Island | 2 |
| Arden Heights | 2 |
| Jamaica Bay | 3 |
| Green-Wood Cemetery | 3 |

**Answer: Governor's Island/Ellis Island/Liberty Island** (only 1 trip)

---
