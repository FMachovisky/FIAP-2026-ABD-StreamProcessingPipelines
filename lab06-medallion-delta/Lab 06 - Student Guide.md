# Lab 06 - Medallion Architecture and Delta Lake with Spark Streaming

**Course / Track:** MBA in Data Engineering (ABD) — Stream Processing & Pipelines (SPP)  
**Environment:** Databricks Free Edition ([login.databricks.com](https://login.databricks.com/))  
**Language / Stack:** Python 3.11+ / PySpark Structured Streaming / Delta Lake / Spark SQL  
**Estimated Duration:** 25 to 30 minutes  

---

## 🎯 Lab Objectives

In this laboratory, you will implement an end-to-end real-time **Medallion Architecture pipeline (Bronze $\rightarrow$ Silver)**, utilizing **Delta Lake** as the transactional Lakehouse storage layer with ACID guarantees and fault tolerance.

By the end of this lab, you will be able to:
1. Ingest raw streaming JSON data directly into **Delta Lake** (**Bronze Layer**).
2. Consume the Bronze Delta table as a continuous streaming source.
3. Apply cleansing, temporal audit enrichment, and null filtering in the **Silver Layer**.
4. Understand the role of `checkpointLocation` in enforcing *Exactly-Once* semantics across streaming hops.
5. Audit micro-batch operations by querying the **Delta Transaction Log** (`DESCRIBE HISTORY`).

---

## 📋 Prerequisites & Materials

* Active account on **Databricks Free Edition** ([login.databricks.com](https://login.databricks.com/)).
* Active compute cluster attached to the workspace.
* Volume **`checkpoint`** available in catalog `workspace` / schema `default`.
* Built-in event stream dataset: `/databricks-datasets/structured-streaming/events/`
* Exercise notebook: [`Lab 06 - Arquitetura Medallion e Delta Lake com Spark Streaming.ipynb`](./Lab%2006%20-%20Arquitetura%20Medallion%20e%20Delta%20Lake%20com%20Spark%20Streaming.ipynb)

---

## 🚀 Step-by-Step Guide

### Step 1: Access and Notebook Import
1. Log into Databricks at [https://login.databricks.com/](https://login.databricks.com/).
2. In the left navigation panel, click **Workspace** -> **Users** -> your user email.
3. Import `Lab 06 - Arquitetura Medallion e Delta Lake com Spark Streaming.ipynb` using **Drag & Drop**.
4. Attach the notebook to your active compute cluster.

### Step 2: Path Configuration & Initial Environment Reset
Define target Delta storage locations and checkpoint directories:
```python
from pyspark.sql.types import StructType, StructField, TimestampType, StringType
from pyspark.sql.functions import col, current_timestamp, desc

# 1. Stop existing active streams
for stream in spark.streams.active:
    stream.stop()

# 2. Path configurations in Volume / DBFS
path_bronze = "/Volumes/workspace/default/checkpoint/lab06_delta/bronze"
path_silver = "/Volumes/workspace/default/checkpoint/lab06_delta/silver"
checkpoint_bronze = "/Volumes/workspace/default/checkpoint/lab06_checkpoints/bronze"
checkpoint_silver = "/Volumes/workspace/default/checkpoint/lab06_checkpoints/silver"

# 3. Clean prior runs for deterministic execution
dbutils.fs.rm("/Volumes/workspace/default/checkpoint/lab06_delta", True)
dbutils.fs.rm("/Volumes/workspace/default/checkpoint/lab06_checkpoints", True)
```

### Step 3: BRONZE Layer (Raw Ingestion $\rightarrow$ Delta Lake)
Read raw JSON files incrementally and append directly to Delta format without mutating the payload:
```python
inputPath = "/databricks-datasets/structured-streaming/events/"

jsonSchema = StructType([
    StructField("time", TimestampType(), True),
    StructField("action", StringType(), True)
])

# Continuous stream ingestion from raw JSON files
df_raw = (
    spark.readStream
        .schema(jsonSchema)
        .option("maxFilesPerTrigger", 1)
        .json(inputPath)
)

# Stream write in Delta format (Bronze)
query_bronze = (
    df_raw.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_bronze)
        .trigger(availableNow=True)
        .start(path_bronze)
)

query_bronze.awaitTermination()
print("Bronze layer processed successfully!")
```

### Step 4: SILVER Layer (Bronze Stream $\rightarrow$ Transformation $\rightarrow$ Delta Silver)
Stream from the Bronze Delta table, append audit metadata, filter malformed events, and persist to Silver:
```python
# 1. Stream from Bronze Delta table
df_bronze_stream = (
    spark.readStream
        .format("delta")
        .load(path_bronze)
)

# 2. Transformations: Audit ingestion timestamp and filter null actions
df_silver = (
    df_bronze_stream
        .withColumn("processamento_ts", current_timestamp())
        .filter(col("action").isNotNull())
)

# 3. Write to Silver Delta table
query_silver = (
    df_silver.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_silver)
        .trigger(availableNow=True)
        .start(path_silver)
)

query_silver.awaitTermination()
print("Silver layer processed successfully!")
```

### Step 5: Validate Medallion Pipeline in SQL
Query the refined Silver table ordered by newest ingestion timestamp:
```python
# Query Silver table
df_silver_resultado = spark.read.format("delta").load(path_silver)
display(df_silver_resultado.orderBy(desc("processamento_ts")))
```

### Step 6: Audit ACID Transactions (Delta History)
Delta Lake logs each streaming batch as an atomic transaction. Audit operational commits:
```sql
%sql
DESCRIBE HISTORY delta.`/Volumes/workspace/default/checkpoint/lab06_delta/silver`
```

### Step 7: Graceful Query Shutdown
```python
query_bronze.stop()
query_silver.stop()
print("Bronze and Silver stream queries terminated cleanly.")
```

---

## 🧪 Validation & Acceptance Criteria

To confirm that the real-time Medallion pipeline succeeded:
1. Target directories `path_bronze` and `path_silver` must contain Parquet data files alongside a `_delta_log/` transaction directory.
2. The Silver dataset must contain the derived audit column `processamento_ts` populated across all rows.
3. Executing `DESCRIBE HISTORY` on the Silver table must return atomic `STREAMING UPDATE` operations corresponding to consumed micro-batches.

---

## 🧹 Environment Cleanup

To release storage on the Databricks volume:
```python
# Cleanup Delta tables and checkpoints for Lab 06
dbutils.fs.rm("/Volumes/workspace/default/checkpoint/lab06_delta", True)
dbutils.fs.rm("/Volumes/workspace/default/checkpoint/lab06_checkpoints", True)
```

---

## 💡 Advanced Challenges

1. **GOLD Layer (Business Aggregations):** Build a third streaming/batch hop (Gold) reading from the Silver Delta table to summarize event volume grouped by hour and action category.
2. **Delta Time Travel:** Use `spark.read.format("delta").option("versionAsOf", 0).load(path_silver)` to inspect earlier historical snapshots of the table.
