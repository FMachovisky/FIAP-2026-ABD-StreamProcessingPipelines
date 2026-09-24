# Lab 04 - Advanced Windowing and Watermarking in Apache Spark Streaming

**Course / Track:** MBA in Data Engineering (ABD) — Stream Processing Pipelines (SPP)  
**Environment:** Databricks Free Edition ([login.databricks.com](https://login.databricks.com/))  
**Language / Stack:** Python 3.11+ / PySpark Structured Streaming / Spark SQL  
**Estimated Duration:** 25 to 30 minutes  

---

## 🎯 Lab Objectives

In this laboratory, you will learn how to handle advanced temporal aggregations and late-arriving records using **Sliding Windows** and **Watermarking** in Apache Spark Structured Streaming.

By the end of this lab, you will be able to:
1. Configure temporal attributes with `TimestampType` to enable true **Event Time** processing.
2. Differentiate between **Tumbling Windows** (fixed intervals) and **Sliding Windows** (overlapping intervals).
3. Apply `withWatermark()` to bound in-memory state retention and drop late-arriving events.
4. Observe event overlap across adjacent windows caused by slide interval duration.
5. Query, dissect, and order time intervals (`window.start` and `window.end`) using **Spark SQL**.

---

## 📋 Prerequisites & Materials

* Active account on **Databricks Free Edition** ([login.databricks.com](https://login.databricks.com/)).
* Active compute cluster attached to the workspace.
* Volume **`checkpoint`** available in catalog `workspace` / schema `default`.
* Built-in event stream dataset: `/databricks-datasets/structured-streaming/events/`
* Exercise notebook: [`Lab 04 - Janelamento Avancado e Watermarking no Apache Spark Streaming.ipynb`](./Lab%2004%20-%20Janelamento%20Avancado%20e%20Watermarking%20no%20Apache%20Spark%20Streaming.ipynb)

---

## 🚀 Step-by-Step Guide

### Step 1: Access and Notebook Import
1. Log into Databricks at [https://login.databricks.com/](https://login.databricks.com/).
2. In the left navigation panel, click **Workspace** -> **Users** -> your user email.
3. Import `Lab 04 - Janelamento Avancado e Watermarking no Apache Spark Streaming.ipynb` using **Drag & Drop**.
4. Attach the notebook to your active compute cluster.

### Step 2: Explicit Schema with Event Timestamp
Temporal windowing requires an event timestamp column explicitly typed as `TimestampType`:
```python
from pyspark.sql.types import StructType, StructField, TimestampType, StringType
from pyspark.sql.functions import col, window

inputPath = "/databricks-datasets/structured-streaming/events/"

# 1. Explicit schema definition
jsonSchema = StructType([
    StructField("time", TimestampType(), True),
    StructField("action", StringType(), True)
])

# 2. Continuous read throttling rate to 1 file per trigger
df_streaming = (
    spark.readStream
        .schema(jsonSchema)
        .option("maxFilesPerTrigger", 1)
        .json(inputPath)
)
```

### Step 3: Watermarking and Sliding Window Aggregation
Configure a 10-minute late-arrival tolerance watermark and a 10-minute window that slides every 5 minutes:
```python
# 3. 10-minute watermark + sliding window (duration: 10 min, slide: 5 min)
df_windowed = (
    df_streaming
        .withWatermark("time", "10 minutes")
        .groupBy(
            window(col("time"), "10 minutes", "5 minutes"),
            col("action")
        )
        .count()
)
```
> **Core Concept:** Because slide duration (5 min) is smaller than window size (10 min), intermediate records are aggregated into **two consecutive windows** (temporal overlap).

### Step 4: Start Query with Checkpoint Reset
Configure the in-memory sink and clear previous checkpoint state for deterministic runs:
```python
# Terminate existing active streams
for stream in spark.streams.active:
    stream.stop()

# Reset Lab 04 checkpoint directory
checkpoint_path = "/Volumes/workspace/default/checkpoint/lab04_checkpoint"
dbutils.fs.rm(checkpoint_path, True)

query = (
    df_windowed.writeStream
        .format("memory")
        .queryName("contagem_janelada")
        .outputMode("complete")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
)
```

> [!IMPORTANT]
> **State Store Bounding and Fault Tolerance:** Watermarking sets the threshold after which Spark discards historical event states from memory (`max(eventTime) - watermarkDelay`). Without watermarking, state stores grow indefinitely, eventually triggering *Out of Memory (OOM)* errors.

### Step 5: Dissect Window Intervals in SQL
Query table `contagem_janelada` extracting lower and upper window bounds:
```sql
%sql
SELECT 
    window.start AS window_start,
    window.end AS window_end,
    action,
    count AS total_events
FROM contagem_janelada
ORDER BY window_start DESC, action
```

### Step 6: Graceful Query Shutdown
Stop the active query stream before moving to subsequent labs:
```python
# Check status and stop
print(f"Query active: {query.isActive}")
query.stop()
```

---

## 🧪 Validation & Acceptance Criteria

To confirm that sliding windows and watermarking functioned properly:
1. SQL query results must output `window_start` and `window_end` boundaries spaced by 10 minutes and shifted by 5-minute increments.
2. Events falling into overlapping boundaries must be accounted for across adjacent window rows.
3. Micro-batch telemetry in `query.lastProgress` must confirm the presence and advancement of the `watermark` property inside `eventTime`.

---

## 🧹 Environment Cleanup

1. **Delete Checkpoint State:** Remove directories inside Volume `checkpoint`.
2. **Delete Notebook:** In **Workspace** -> `Users` -> remove the Lab 04 notebook.

---

## 💡 Advanced Challenges

1. **Comparison with Tumbling Windows:** Switch the aggregation window to a fixed, non-sliding interval:
   ```python
   df_windowed = (
       df_streaming
           .withWatermark("time", "10 minutes")
           .groupBy(
               window(col("time"), "10 minutes"), # Fixed 10-minute tumbling window
               col("action")
           )
           .count()
   )
   ```
   *Rerun and observe how intervals become non-overlapping contiguous slices (`15:00-15:10`, `15:10-15:20`).*

2. **Watermark Progression Inspection:** Inspect `display(query.lastProgress)` in Python to examine the internal watermark timestamp progression across micro-batches.
