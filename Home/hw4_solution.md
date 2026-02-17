# Homework 04 Solutions: Analytics Engineering with dbt

## Question 1: dbt Lineage and Execution
Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

- `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned` (upstream dependencies)
- Any model with upstream and downstream dependencies to `int_trips_unioned`
- `int_trips_unioned` only
- `int_trips_unioned`, `int_trips`, and `fct_trips` (downstream dependencies)

---
**Answer: `int_trips_unioned` only**

**Explanation:**  
The `--select` flag in dbt targets only the specified model. It does not include upstream or downstream dependencies automatically. To include upstream dependencies, you would need the `+` prefix: `dbt run --select +int_trips_unioned`. To include downstream, you'd use the `+` suffix: `dbt run --select int_trips_unioned+`. Without the `+` operator, dbt builds only the explicitly selected model.

## Question 2: dbt Tests
You've configured a generic test like this in your `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

- dbt will skip the test because the model didn't change
- dbt will fail the test, returning a non-zero exit code
- dbt will pass the test with a warning about the new value
- dbt will update the configuration to include the new value

---
**Answer: dbt will fail the test, returning a non-zero exit code**

**Explanation:**  
The `accepted_values` test checks that every value in the column exists in the specified list `[1, 2, 3, 4, 5]`. When value `6` appears, it is not in the accepted list, so the test fails. dbt does not skip tests based on model changes—it always evaluates the current data. It also does not auto-update configurations. The test returns a non-zero exit code indicating failure.

## Question 3: Counting Records in `fct_monthly_zone_revenue`
`fct_monthly_zone_revenue`

After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

- 12,998
- 14,120
- 12,184
- 15,421

---
**SQL Query:**

```sql
SELECT COUNT(*) AS record_count
FROM fct_monthly_zone_revenue;
```
**Answer: 12,998**

**Explanation:**  
The `fct_monthly_zone_revenue` model aggregates trips by `pickup_zone`, `revenue_month` (truncated to month), and `service_type` (Green/Yellow). For the 2019–2020 green and yellow taxi dataset, this produces 12,998 unique zone-month-service combinations.

## Question 4: Best Performing Zone for Green Taxis (2020)
Using the `fct_monthly_zone_revenue` table, find the pickup zone with the **highest total revenue** (`revenue_monthly_total_amount`) for **Green** taxi trips in 2020.

Which zone had the highest revenue?

- East Harlem North
- Morningside Heights
- East Harlem South
- Washington Heights South

---
**SQL Query:**

```sql
SELECT
    pickup_zone,
    SUM(revenue_monthly_total_amount) AS total_revenue
FROM fct_monthly_zone_revenue
WHERE service_type = 'Green'
  AND revenue_month >= '2020-01-01'
  AND revenue_month < '2021-01-01'
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 5;
```
**Answer: East Harlem North**

**Explanation:**  
Among the four options—East Harlem North, Morningside Heights, East Harlem South, and Washington Heights South—East Harlem North had the highest total revenue (`revenue_monthly_total_amount`) for Green taxi trips during the year 2020.

## Question 5: Green Taxi Trip Counts (October 2019)
Using the `fct_monthly_zone_revenue` table, what is the **total number of trips** (`total_monthly_trips`) for Green taxis in October 2019?

- 500,234
- 350,891
- 384,624
- 421,509
---
**SQL Query:**

```sql
SELECT
    SUM(total_monthly_trips) AS total_trips
FROM fct_monthly_zone_revenue
WHERE service_type = 'Green'
  AND revenue_month = '2019-10-01';
```
**Answer: 384,624**

**Explanation:**  
Summing `total_monthly_trips` across all pickup zones for Green taxis where `revenue_month` is October 2019 (truncated to `2019-10-01`) yields 384,624 total trips.

## Question 6: To build a Staging Model for FHV Data

Create a staging model for the **For-Hire Vehicle (FHV)** trip data for 2019.

1. Load the [FHV trip data for 2019](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
   - Filter out records where `dispatching_base_num IS NULL`
   - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in `stg_fhv_tripdata`?

- 42,084,899
- 43,244,696
- 22,998,722
- 44,112,187

---
### To solve the above question, follow the following steps

### Step 1: Add FHV source to `sources.yml`

Add the following code to the file name `models/staging/sources.yml`:

```yaml
      - name: fhv_tripdata
        description: Raw For-Hire Vehicle (FHV) trip records for 2019
        columns:
          - name: dispatching_base_num
            description: The TLC base license number of the dispatching base
          - name: pickup_datetime
            description: Date and time of the trip pickup
          - name: dropOff_datetime
            description: Date and time of the trip dropoff
          - name: PUlocationID
            description: TLC Taxi Zone where the trip began
          - name: DOlocationID
            description: TLC Taxi Zone where the trip ended
          - name: SR_Flag
            description: Shared ride flag (1 = shared ride)
          - name: Affiliated_base_number
            description: The TLC base license number affiliated with the trip
```

### Step 2: Then create the `stg_fhv_tripdata.sql` model

from the companion file: [`stg_fhv_tripdata.sql`](stg_fhv_tripdata.sql)

```sql
-- models/staging/stg_fhv_tripdata.sql

with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers
        dispatching_base_num,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(sr_flag as integer) as sr_flag,
        affiliated_base_number

    from source
    -- Filter out records where dispatching_base_num is null (data quality requirement)
    where dispatching_base_num is not null
)

select * from renamed
```

### Step 3: Add schema definition

Add the following code in the file named `models/staging/schema.yml`:

```yaml
  - name: stg_fhv_tripdata
    description: >
      Staging model for For-Hire Vehicle (FHV) trip data (2019).
      Standardizes column names, casts data types, and filters out
      records where dispatching_base_num is null.
    columns:
      - name: dispatching_base_num
        description: The TLC base license number of the dispatching base
        data_tests:
          - not_null
      - name: pickup_location_id
        description: TLC Taxi Zone where the trip began
      - name: dropoff_location_id
        description: TLC Taxi Zone where the trip ended
      - name: pickup_datetime
        description: Date and time of the trip pickup
      - name: dropoff_datetime
        description: Date and time of the trip dropoff
      - name: sr_flag
        description: Shared ride flag (1 = shared ride)
      - name: affiliated_base_number
        description: The TLC base license number affiliated with the trip
```

### Step 4: Running the model

```bash
dbt run --select stg_fhv_tripdata
```

### Step 5: Counting records

```sql
SELECT COUNT(*) AS record_count
FROM stg_fhv_tripdata;
```

**Answer: 43,244,696**

**Explanation:**  
The 2019 FHV trip data contains approximately 43.2 million records. After filtering out rows where `dispatching_base_num IS NULL`, the count is 43,244,696.
