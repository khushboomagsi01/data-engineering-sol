# Homework Solutions - Data Warehouse

---

## Question 1: Total Record Count

**Question:** What is the count of records for the 2024 Yellow Taxi Data?

**Query:**
```sql
SELECT COUNT(*) as total_records 
FROM yellow_taxi_2024;
```

**Answer:** **20,332,093** 

**Explanation:** The dataset contains 20,332,093 records across the January-June 2024 period.


## Question 2: Estimated Data Read for Distinct PULocationIDs

**Question:** What is the estimated amount of data that will be read when querying distinct PULocationIDs on both tables?

**Query:**
```sql
SELECT COUNT(DISTINCT PULocationID) as distinct_locations
FROM yellow_taxi_2024;
```

**Result:** Found 262 distinct PULocationIDs

**Answer:** **0 MB for the External Table and 155.12 MB for the Materialized Table**

**Explanation:**
- External tables don't show accurate size estimates upfront (BigQuery shows 0 MB)
- The materialized table shows the actual column size to be scanned (~155 MB for PULocationID)
- In our analysis: 262 distinct pickup locations across 20.3M records


## Question 3: Column-based Storage Explanation

**Question:** Why are the estimated number of bytes different when querying one column vs two columns?

**Answer:** **BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.** 

**Explanation:**
BigQuery uses columnar storage:
- Query 1 (PULocationID only): Scans ~155 MB
- Query 2 (PULocationID + DOLocationID): Scans ~310 MB (approximately 2x)
This demonstrates that BigQuery reads only the columns you request, not entire rows.


## Question 4: Records with Zero Fare Amount

**Question:** How many records have a fare_amount of 0?

**Query:**
```sql
SELECT COUNT(*) as zero_fare_count
FROM yellow_taxi_2024
WHERE fare_amount = 0;
```

**Answer:** **8,333** 

**Explanation:** There are exactly 8,333 records where the fare_amount equals 0 in the Jan-June 2024 dataset.


## Question 5: Optimization Strategy

**Question:** What is the best strategy to optimize the table for queries that filter by tpep_dropoff_datetime and order by VendorID?

**Answer:** **Partition by tpep_dropoff_datetime and Cluster on VendorID** ✓

**Explanation:**
- **Partition by tpep_dropoff_datetime**: Filtering by date ranges will scan only relevant date partitions, reducing data scanned
- **Cluster by VendorID**: Ordering by VendorID will be faster as data is already organized by this column
- This strategy optimizes both the WHERE clause (partition pruning) and ORDER BY (clustering)



## Question 6: Partition Performance Comparison

**Question:** Compare estimated bytes for querying distinct VendorIDs between specific dates on non-partitioned vs partitioned tables.

**Query:**
```sql
-- Count records in date range to estimate partition size
SELECT 
    COUNT(*) as records_in_range,
    COUNT(DISTINCT VendorID) as distinct_vendors
FROM yellow_taxi_2024
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```


**Answer:** **310.24 MB for non-partitioned table and 26.84 MB for the partitioned table** 

**Explanation:**
- Date range March 1-15 represents approximately 8% of the total data (1,630,613 records out of 20,332,093)
- Non-partitioned table: Must scan entire dataset (~310 MB)
- Partitioned table: Scans only March 1-15 partitions (~27 MB, or ~8% of total)
- **Cost savings: ~91%** by using partitioning for date-range queries



## Question 7: External Table Storage Location

**Question:** Where is the data stored in the External Table you created?

**Answer:** **GCP Bucket** 

**Explanation:**
External tables in BigQuery don't store data within BigQuery itself. They create a reference to data stored in Google Cloud Storage (GCS) buckets. The data remains in the GCS bucket as parquet files, and BigQuery queries it directly from there without importing or copying it.


## Question 8: Clustering Best Practice

**Question:** It is best practice in Big Query to always cluster your data: True or False?

**Answer:** **False** 

**Explanation:**
Clustering is NOT always the best practice. It depends on:
- **Table size**: Clustering benefits tables larger than 1 GB. Small tables may see overhead > benefits
- **Query patterns**: Most useful when queries filter/group by high-cardinality columns
- **Data usage**: Not beneficial for full table scans or rarely-queried tables
- **Update frequency**: Frequently updated tables may have clustering overhead


## Question 9 (Bonus): Count Query Estimate

**Question:** Write a `SELECT COUNT(*)` query on the materialized table. How many bytes does it estimate? Why?

**Answer:** **0 Bytes** 


