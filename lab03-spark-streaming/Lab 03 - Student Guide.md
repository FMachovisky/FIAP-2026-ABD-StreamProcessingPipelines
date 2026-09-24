# Lab 03 - Foundations of Apache Spark Streaming (Structured Streaming)

**Course / Track:** MBA in Data Engineering (ABD) — Stream Processing Pipelines (SPP)  
**Environment:** Databricks Free Edition ([login.databricks.com](https://login.databricks.com/))  
**Language / Stack:** Python 3.11+ / PySpark Structured Streaming / Spark SQL  
**Estimated Duration:** 25 to 30 minutes  

---

## 🎯 Lab Objectives

In this laboratory, you will learn how to design, execute, and monitor your first **Real-Time Data Streaming Pipeline** using the Apache Spark **Structured Streaming API** in Databricks.

By the end of this lab, you will be able to:
1. Create and configure a **Checkpointing Volume** in Unity Catalog for state persistence and *exactly-once* guarantees.
2. Configure a continuous ingestion stream with `spark.readStream`, applying an **explicit schema** and throttling rate with `maxFilesPerTrigger`.
3. Apply streaming window aggregations over 1-hour tumbling intervals (`window`).
4. Initialize and control the streaming query lifecycle via `.writeStream` (`complete` mode, in-memory sink, and checkpointing).
5. Query the active in-memory table interactively using **Spark SQL** and observe stream progression.
6. Perform graceful query shutdown and cloud resource cleanup.

---

## 📋 Prerequisites & Materials

* Active account on **Databricks Free Edition** ([login.databricks.com](https://login.databricks.com/)).
* Active compute cluster attached to the workspace.
* Built-in Databricks event stream dataset: `/databricks-datasets/structured-streaming/events/`
* Exercise notebook: [`Lab 03 - Exemplo Basico Utilizando Apache Spark Streaming.ipynb`](./Lab%2003%20-%20Exemplo%20Basico%20Utilizando%20Apache%20Spark%20Streaming.ipynb)

---

## 🚀 Step-by-Step Guide

### Step 1: Access and Notebook Import
1. Log into Databricks at [https://login.databricks.com/](https://login.databricks.com/).
2. In the left navigation panel, click **Workspace** -> **Users** -> your user email.
3. Import the notebook `Lab 03 - Exemplo Basico Utilizando Apache Spark Streaming.ipynb` using **Drag & Drop**.

### Step 2: Volume Creation for Checkpointing
Structured Streaming requires durable storage for offsets and Write-Ahead Logs (WAL):
1. In the left navigation bar, open **Catalog**.
2. Select catalog **`workspace`** and schema **`default`**.
3. Create a new Volume named exactly **`checkpoint`** (target path: `/Volumes/workspace/default/checkpoint/`).

### Step 3: Inspect Upstream Stream Source
In the first notebook cell, inspect the 50 JSON event files provided by Databricks:
```python
# Listing example dataset directory
display(dbutils.fs.ls('/databricks-datasets/structured-streaming/events/'))
```

### Step 4: Explicit Schema & Continuous Ingestion (`readStream`)
In Structured Streaming, runtime schema inference is disabled for stability and deterministic processing:
```python
from pyspark.sql.types import StructType, StructField, TimestampType, StringType
from pyspark.sql.functions import window

inputPath = "/databricks-datasets/structured-streaming/events/"

# 1. Defining explicit schema
jsonSchema = StructType([
    StructField("time", TimestampType(), True),
    StructField("action", StringType(), True)
])

# 2. Ingest stream (throttling 1 file per micro-batch trigger)
streamingInputDF = (
    spark.readStream
        .schema(jsonSchema)
        .option("maxFilesPerTrigger", 1)
        .json(inputPath)
)

# 3. Temporal window aggregation by action and 1-hour interval
streamingCountsDF = (
    streamingInputDF
        .groupBy(
            streamingInputDF.action,
            window(streamingInputDF.time, "1 hour")
        )
        .count()
)
```

### Step 5: Start Streaming Query (`writeStream`)
Configure the in-memory sink (`memory`), output mode (`complete`), and point to the checkpoint location:
```python
checkpoint_path = "/Volumes/workspace/default/checkpoint/contagem_checkpoint"

# Clear previous checkpoint state to allow repeatable runs
dbutils.fs.rm(checkpoint_path, True)

query = (
    streamingCountsDF
        .writeStream
        .format("memory")
        .queryName("contagem")
        .outputMode("complete")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)  # Process available backlog and terminate
        .start()
)
```

> [!IMPORTANT]
> **Technical Notice on Databricks Free / Serverless Triggers:**  
> Free Edition and Serverless runtimes prohibit infinite continuous streaming via `ProcessingTime` (e.g. `.trigger(processingTime="10s")`), returning `[INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED]`.  
> Using **`.trigger(availableNow=True)`** is required: it processes all pending micro-batches at the configured rate (`maxFilesPerTrigger`) and terminates cleanly when the queue is exhausted.

### Step 6: Interactive SQL Queries & Visualization
Query the in-memory table while micro-batches are consumed:
```sql
%sql
SELECT 
    action, 
    date_format(window.end, "MMM-dd HH:mm") AS time, 
    count 
FROM contagem 
ORDER BY time, action
```
> **Visualization Tip:** Click **+** / **Visualization** below the SQL cell to configure a **Grouped Bar Chart** with `time` on the X-axis and grouped by `action`.

### Step 7: Graceful Query Termination
Ensure the stream query is shut down when finished:
```python
# Check query execution status
print(f"Query active: {query.isActive}")

# Stop query execution
query.stop()
```

---

## 🧪 Validation & Acceptance Criteria

To confirm that the streaming pipeline executed successfully:
1. The streaming query must initialize without schema errors or checkpoint access denials.
2. The `%sql SELECT ... FROM contagem` query must return aggregated count rows for each time window.
3. Upon consuming all pending input files (`availableNow=True`), `query.isActive` must evaluate to `False`.

---

## 🧹 Environment Cleanup

To free storage and avoid conflicts in subsequent labs:
1. **Delete Checkpoint Volume:** In **Catalog** -> `workspace` -> `default` -> delete Volume **`checkpoint`**.
2. **Delete Notebook:** In **Workspace** -> `Users` -> delete the Lab 03 notebook.

---

## 💡 Advanced Challenges

1. **Streaming Metrics Telemetry:** Use `query.lastProgress` or `query.recentProgress` in Python to monitor `inputRowsPerSecond`, `processedRowsPerSecond`, and execution latency per micro-batch.
2. **Business Filtering:** Add a `.filter(col("action") == "Open")` before the `groupBy` transformation and inspect the resulting aggregated values in SQL.
