# Workshop Homework Solutions

## Question 1: What is the start date and end date of the dataset?

**Query:**

```sql
SELECT
    MIN(trip_pickup_date_time) AS start_date,
    MAX(trip_pickup_date_time) AS end_date
FROM taxi_data.rides;
```

**Result:**

| start_date | end_date |
|---|---|
| 2009-06-01 | 2009-07-01 |

**Answer: 2009-06-01 to 2009-07-01** ✅



## Question 2: What proportion of trips are paid with credit card?

**Query:**

```sql
SELECT
    payment_type,
    COUNT(*) AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM taxi_data.rides
GROUP BY payment_type
ORDER BY cnt DESC;
```

**Result:**

| payment_type | cnt | pct |
|---|---|---|
| CASH | 7235 | 72.35% |
| Credit | 2666 | 26.66% |
| Cash | 97 | 0.97% |
| No Charge | 1 | 0.01% |
| Dispute | 1 | 0.01% |

**Answer: 26.66%** ✅

## Question 3: What is the total amount of money generated in tips?

**Query:**

```sql
SELECT ROUND(SUM(tip_amt), 2) AS total_tips
FROM taxi_data.rides;
```

**Result:**

| total_tips |
|---|
| $6,063.41 |

**Answer: $6,063.41** ✅
