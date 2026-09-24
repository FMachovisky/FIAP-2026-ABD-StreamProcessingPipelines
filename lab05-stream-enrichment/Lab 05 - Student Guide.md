# Lab 05 - Real-Time Stream Enrichment (Stream-Static Join)

**Course / Track:** MBA in Data Engineering (ABD) — Stream Processing Pipelines (SPP)  
**Environment:** Databricks Free Edition ([login.databricks.com](https://login.databricks.com/))  
**Language / Stack:** Python 3.11+ / PySpark Structured Streaming / Spark SQL  
**Estimated Duration:** 25 to 30 minutes  

---

## 🎯 Lab Objectives

In this laboratory, you will implement the enterprise architectural pattern of **Real-Time Stream Lookup and Enrichment (*Stream-Static Join*)**, joining a continuous incoming event stream (*Streaming DataFrame*) with a reference dimensional dataset (*Static DataFrame*).

By the end of this lab, you will be able to:
1. Create and manage static reference dimension tables in Spark memory.
2. Execute real-time `join` operations between streaming inputs and static DataFrames.
3. Understand how Spark broadcasts and replicates static dimension tables across workers to enrich micro-batches without shuffles.
4. Query enriched real-time datasets with dimension attributes using **Spark SQL**.

---

## 📋 Prerequisites & Materials

* Active account on **Databricks Free Edition** ([login.databricks.com](https://login.databricks.com/)).
* Active compute cluster attached to the workspace.
* Volume **`checkpoint`** available in catalog `workspace` / schema `default`.
* Built-in event stream dataset: `/databricks-datasets/structured-streaming/events/`
* Exercise notebook: [`Lab 05 - Enriquecimento de Dados em Tempo Real com Spark Streaming.ipynb`](./Lab%2005%20-%20Enriquecimento%20de%20Dados%20em%20Tempo%20Real%20com%20Spark%20Streaming.ipynb)

---

## 🚀 Step-by-Step Guide

### Step 1: Access and Notebook Import
1. Log into Databricks at [https://login.databricks.com/](https://login.databricks.com/).
2. In the left navigation panel, click **Workspace** -> **Users** -> your user email.
3. Import `Lab 05 - Enriquecimento de Dados em Tempo Real com Spark Streaming.ipynb` using **Drag & Drop**.
4. Attach the notebook to your active compute cluster.

### Step 2: Build Reference Dimension (Static DataFrame)
Create an in-memory reference dimension table providing business descriptions and priority rankings:
```python
from pyspark.sql.types import StructType, StructField, TimestampType, StringType
from pyspark.sql.functions import col

# 1. Static reference dataset (Lookup / Dimension)
static_data = [
    ("Open", "Abertura de Sessão", "Alta Prioridade"),
    ("Close", "Encerramento de Sessão", "Média Prioridade")
]

df_static = spark.createDataFrame(
    static_data, 
    ["action", "descricao", "prioridade"]
)

display(df_static)
```

### Step 3: Stream Ingestion & Real-Time Join
Configure continuous stream ingestion and join against the static dimension via key `action`:
```python
inputPath = "/databricks-datasets/structured-streaming/events/"

# 2. Explicit event schema
jsonSchema = StructType([
    StructField("time", TimestampType(), True),
    StructField("action", StringType(), True)
])

# 3. Stream ingestion
df_streaming = (
    spark.readStream
        .schema(jsonSchema)
        .option("maxFilesPerTrigger", 1)
        .json(inputPath)
)

# 4. Stream-Static Join: Spark broadcasts the static dimension to enrich each micro-batch
df_enriched = df_streaming.join(df_static, "action")
```

### Step 4: Start Stream Enrichment Query
Configure an in-memory sink with dedicated checkpointing in `append` mode:
```python
# Terminate existing active streams
for stream in spark.streams.active:
    stream.stop()

# Reset Lab 05 checkpoint directory
checkpoint_path = "/Volumes/workspace/default/checkpoint/lab05_checkpoint"
dbutils.fs.rm(checkpoint_path, True)

query = (
    df_enriched.writeStream
        .format("memory")
        .queryName("eventos_enriquecidos")
        .outputMode("append")  # Append mode for row-by-row enriched event output
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
)
```

### Step 5: Query Enriched Records in SQL
Examine enriched output records with merged `descricao` and `prioridade` attributes:
```sql
%sql
SELECT 
    time, 
    action, 
    descricao, 
    prioridade 
FROM eventos_enriquecidos 
ORDER BY time DESC 
LIMIT 20
```

### Step 6: Graceful Query Termination
```python
# Terminate query execution
print(f"Query status before stopping: {query.status}")
query.stop()
print("Enrichment stream terminated successfully.")
```

---

## 🧪 Validation & Acceptance Criteria

To confirm that real-time stream enrichment executed successfully:
1. Every row produced in `eventos_enriquecidos` must contain both raw event fields and enriched dimension fields (`descricao` and `prioridade`).
2. SQL queries must verify that `"Open"` actions resolve to `"Abertura de Sessão"` and `"Close"` actions to `"Encerramento de Sessão"`.
3. Output mode must operate in `append`, ensuring incremental arrival without overwriting earlier processed events.

---

## 🧹 Environment Cleanup

1. **Delete Checkpoint State:** Remove directories inside Volume `checkpoint`.
2. **Delete Notebook:** In **Workspace** -> `Users` -> remove the Lab 05 notebook.

---

## 💡 Advanced Challenges

1. **Priority Filtering:** Add an analytical filter after the join to retain only records classified as `"Alta Prioridade"`:
   ```python
   df_alta_prioridade = df_enriched.filter(col("prioridade") == "Alta Prioridade")
   ```
2. **Left Outer Join:** Change join type to `df_streaming.join(df_static, "action", "left")` and observe how Spark emits null attributes for unmatched incoming event types.
