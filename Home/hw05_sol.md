# Data Platforms with Bruin


### Question 1. Bruin Pipeline Structure

**In a Bruin project, what are the required files/directories?**

**Answer:** `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`

**Explanation:**  
From the [Bruin Concepts docs](https://bruin-data.github.io/bruin/getting-started/concepts.html), a pipeline is defined with a `pipeline.yml` file, and all assets must be under a folder called `assets/` next to it:

```
my-pipeline/
  ├─ pipeline.yml
  └─ assets/
    ├─ asset1.sql
    └─ asset2.py
```

Additionally, `.bruin.yml` is required at the repo root to define connections/credentials. When you first run `bruin validate` or `bruin run`, Bruin creates an empty `.bruin.yml` file automatically.


### Question 2. Materialization Strategies

**Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?**

**Answer:** `time_interval` - incremental based on a time column

**Explanation:**  
From the [Bruin Materialization docs](https://bruin-data.github.io/bruin/assets/materialization.html), the `time_interval` strategy is designed for incrementally loading time-based data within specific time windows. The following steps show how it works:

1. Beginning a transaction
2. Deleting existing records within the specified time interval
3. Inserting new records from the query

It requires an `incremental_key` (the time column) and `time_granularity` (`date` or `timestamp`). You specify the time window with `--start-date` and `--end-date` flags:

```bash
bruin run --start-date "2024-03-01" --end-date "2024-03-31" path/to/your/asset
```


### Question 3. Pipeline Variables

**How do you override the `taxi_types` array variable to only process yellow taxis?**

**Answer:** `bruin run --var 'taxi_types=["yellow"]'`

**Explanation:**  
From the [Bruin Variables docs](https://bruin-data.github.io/bruin/core-concepts/variables.html), you override variables at runtime using the `--var` flag. For complex types like arrays, we use JSON format:

```bash
# Override with JSON for complex types
bruin run --var '{"users": ["alice", "charlie"]}'

# Key=value format also works
bruin run --var 'taxi_types=["yellow"]'
```

Since `taxi_types` is an array variable, the value must remain a JSON array — `["yellow"]` — not just a bare string.


### Question 4. Running with Dependencies

**You've modified the `ingestion/trips.py` asset and want to run it plus all downstream assets. Which command?**

**Answer:** `bruin run ingestion/trips.py --downstream`

**Explanation:**  
From the [Bruin Run Command docs](https://bruin-data.github.io/bruin/commands/run.html), the `--downstream` flag runs the specified asset and all assets that depend on it:

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--downstream` | bool | false | Run all downstream assets as well. |


### Question 5. Quality Checks

**Which quality check ensures `pickup_datetime` never has NULL values?**

**Answer:** `name: not_null`

**Explanation:**  
From the [Bruin Available Checks docs](https://bruin-data.github.io/bruin/quality/available_checks.html), the `not_null` check verifies that none of the values of the checked column are null:

```yaml
columns:
  - name: pickup_datetime
    type: timestamp
    checks:
      - name: not_null
```

### Question 6. Lineage and Dependencies

**Which Bruin command visualizes the dependency graph between assets?**

**Answer:** `bruin lineage`

**Explanation:**  
From the [Bruin Lineage Command docs](https://bruin-data.github.io/bruin/commands/lineage.html), the `lineage` command shows:

- What the asset relies on (upstream dependencies)
- What relies on the asset (downstream dependencies)

```bash
bruin lineage path/to/asset.sql
```



### Question 7. First-Time Run

**What flag should you use to ensure tables are created from scratch?**

**Answer:** `--full-refresh`

**Explanation:**  
From the [Bruin Run Command docs](https://bruin-data.github.io/bruin/commands/run.html) and [Materialization docs](https://bruin-data.github.io/bruin/assets/materialization.html):

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--full-refresh` | bool | false | Truncate the table before running. Also sets the `full_refresh` jinja variable to `True` and `BRUIN_FULL_REFRESH` environment variable to `1`. |

When running with `--full-refresh`, Bruin will drop and recreate tables to ensure a clean state:

```bash
bruin run --full-refresh
```
