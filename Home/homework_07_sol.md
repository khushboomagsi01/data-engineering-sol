# Homework 07 — Solutions

## Question 1. Redpanda version

```bash
docker exec -it workshop-redpanda-1 rpk version
```

**Answer: v25.3.9**

---

## Question 2. Sending data to Redpanda

### Create topic

```bash
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```

### Producer code (`green_producer.py`)

```python
import json
from time import time

import pandas as pd
from kafka import KafkaProducer

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = [
    'lpep_pickup_datetime', 'lpep_dropoff_datetime',
    'PULocationID', 'DOLocationID',
    'passenger_count', 'trip_distance',
    'tip_amount', 'total_amount'
]

df = pd.read_parquet(url, columns=columns)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

t0 = time()

for _, row in df.iterrows():
    msg = {
        'lpep_pickup_datetime': str(row['lpep_pickup_datetime']),
        'lpep_dropoff_datetime': str(row['lpep_dropoff_datetime']),
        'PULocationID': int(row['PULocationID']),
        'DOLocationID': int(row['DOLocationID']),
        'passenger_count': int(row['passenger_count']) if pd.notna(row['passenger_count']) else 0,
        'trip_distance': float(row['trip_distance']),
        'tip_amount': float(row['tip_amount']),
        'total_amount': float(row['total_amount']),
    }
    producer.send('green-trips', value=msg)

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
print(f'sent {len(df)} records')
```

Output: `took ~4 seconds`, sent 49416 records.

**Answer: 10 seconds** (closest option)

## Question 3. Consumer - trip distance

### Consumer code (`green_consumer.py`)

```python
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'green-trips',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='green-trips-counter',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=10000
)

count = 0
total = 0
for message in consumer:
    total += 1
    if message.value['trip_distance'] > 5.0:
        count += 1

consumer.close()
print(f'Total messages: {total}')
print(f'Trips with trip_distance > 5: {count}')
```

Output: `Total messages: 49416`, `Trips with trip_distance > 5: 8506`

**Answer: 8506**

## Question 4. Tumbling window - pickup location

### PostgreSQL table

```sql
CREATE TABLE IF NOT EXISTS green_trips_tumbling (
    window_start TIMESTAMP(3),
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);
```

### Flink job (`green_tumbling_pickup.py`)

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_green_trips_source(t_env):
    source_ddl = """
        CREATE TABLE green_trips (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)


def create_tumbling_sink(t_env):
    sink_ddl = """
        CREATE TABLE green_trips_tumbling (
            window_start TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'green_trips_tumbling',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    create_green_trips_source(t_env)
    create_tumbling_sink(t_env)

    t_env.execute_sql("""
        INSERT INTO green_trips_tumbling
        SELECT
            window_start,
            PULocationID,
            COUNT(*) AS num_trips
        FROM TABLE(
            TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
        )
        GROUP BY window_start, PULocationID;
    """).wait()


if __name__ == '__main__':
    run()
```

Query:

```sql
SELECT PULocationID, num_trips
FROM green_trips_tumbling
ORDER BY num_trips DESC
LIMIT 3;
```

Result:
| PULocationID | num_trips |
|---|---|
| 74 | 15 |
| 74 | 14 |
| 74 | 13 |

**Answer: 74**

## Question 5. Session window - longest streak

### PostgreSQL table

```sql
CREATE TABLE IF NOT EXISTS green_trips_session (
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (session_start, PULocationID)
);
```

### Flink job (`green_session_window.py`)

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_green_trips_source(t_env):
    source_ddl = """
        CREATE TABLE green_trips (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)


def create_session_sink(t_env):
    sink_ddl = """
        CREATE TABLE green_trips_session (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (session_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'green_trips_session',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    create_green_trips_source(t_env)
    create_session_sink(t_env)

    t_env.execute_sql("""
        INSERT INTO green_trips_session
        SELECT
            window_start,
            window_end,
            PULocationID,
            COUNT(*) AS num_trips
        FROM TABLE(
            SESSION(TABLE green_trips PARTITION BY PULocationID, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
        )
        GROUP BY window_start, window_end, PULocationID;
    """).wait()


if __name__ == '__main__':
    run()
```

Query:

```sql
SELECT PULocationID, num_trips
FROM green_trips_session
ORDER BY num_trips DESC
LIMIT 5;
```

**Answer: 81**

## Question 6. Tumbling window - largest tip

### PostgreSQL table

```sql
CREATE TABLE IF NOT EXISTS green_trips_hourly_tips (
    window_start TIMESTAMP(3),
    total_tips DOUBLE PRECISION,
    PRIMARY KEY (window_start)
);
```

### Flink job (`green_hourly_tips.py`)

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_green_trips_source(t_env):
    source_ddl = """
        CREATE TABLE green_trips (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)


def create_hourly_tips_sink(t_env):
    sink_ddl = """
        CREATE TABLE green_trips_hourly_tips (
            window_start TIMESTAMP(3),
            total_tips DOUBLE,
            PRIMARY KEY (window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'green_trips_hourly_tips',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    create_green_trips_source(t_env)
    create_hourly_tips_sink(t_env)

    t_env.execute_sql("""
        INSERT INTO green_trips_hourly_tips
        SELECT
            window_start,
            SUM(tip_amount) AS total_tips
        FROM TABLE(
            TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
        )
        GROUP BY window_start;
    """).wait()


if __name__ == '__main__':
    run()
```

Query:

```sql
SELECT window_start, total_tips
FROM green_trips_hourly_tips
ORDER BY total_tips DESC
LIMIT 5;
```

**Answer: 2025-10-16 18:00:00**
