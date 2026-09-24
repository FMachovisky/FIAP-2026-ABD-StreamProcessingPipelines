# Lab 02 - Batch Ingestion and Processing with Apache Spark (PySpark)

**Course / Track:** MBA in Data Engineering (ABD) — Stream Processing Pipelines (SPP)  
**Environment:** Databricks Free Edition ([login.databricks.com](https://login.databricks.com/))  
**Language / Stack:** Python 3.11+ / Apache Spark (PySpark) / DataFrames API  
**Estimated Duration:** 25 to 30 minutes  

---

## 🎯 Lab Objectives

In this laboratory, you will learn how to perform **Batch data ingestion and processing** using the PySpark **DataFrames API** within Databricks.

By the end of this lab, you will be able to:
1. Provision storage structure (Unity Catalog Volume) in Databricks.
2. Load multiline structured JSON files with schema inference and verification.
3. Apply relational transformations: selection, filtering, and derived computed columns.
4. Execute analytical aggregations and business metric summarizations.
5. Persist data into temporary and permanent tables and validate processed dataset integrity.

---

## 📋 Prerequisites & Materials

* Active account on **Databricks Free Edition** ([login.databricks.com](https://login.databricks.com/)).
* Data file: [`dataset.json`](./dataset.json)
* Exercise notebook: [`Lab 02 - Exemplo de Ingestao e Processamento Batch no Apache Spark.ipynb`](./Lab%2002%20-%20Exemplo%20de%20Ingestao%20e%20Processamento%20Batch%20no%20Apache%20Spark.ipynb)

---

## 🚀 Step-by-Step Guide

### Step 1: Access and Notebook Import
1. Log into Databricks at [https://login.databricks.com/](https://login.databricks.com/).
2. In the left navigation panel, click **Workspace** -> **Users** -> your user email.
3. Import the notebook `Lab 02 - Exemplo de Ingestao e Processamento Batch no Apache Spark.ipynb` using the **Drag & Drop** feature in the import dialog.

### Step 2: Volume Creation and Data Upload
1. In the left sidebar, navigate to **Catalog**.
2. Select catalog **`workspace`** and schema **`default`**.
3. Create a new Volume named **`teste`** (target path: `/Volumes/workspace/default/teste/`).
4. Upload file `dataset.json` by dragging and dropping it into the `teste` Volume.

### Step 3: Validate File Paths and Identifiers
1. Open the imported notebook and attach it to an active compute cluster.
2. Ensure the JSON file location in the ingestion cell matches the created Volume:
   ```python
   file_location = "/Volumes/workspace/default/teste/dataset.json"
   file_type = "json"
   ```
3. Verify table identifiers declared across downstream notebook cells.

### Step 4: Batch Reading and Ingestion
Run the loading cells:

```python
# Batch ingestion with multiline JSON support
df = spark.read.format(file_type) \
    .option("multiline", "true") \
    .option("inferSchema", "true") \
    .load(file_location)

# Display dataset and inferred schema
display(df)
df.printSchema()
```

### Step 5: Transformations and Filtering
1. **Row Filtering:** Filter products above a price threshold (`display(df.filter(df["preco"].cast("double") > 100))`).
2. **Computed Column:** Compute `valor_total_estoque` by multiplying `preco` by `quantidade`:

```python
from pyspark.sql.functions import col, round

df_transformado = df.withColumn("valor_total_estoque", round(col("preco").cast("double") * col("quantidade"), 2))
display(df_transformado)
```

### Step 6: Analytical Aggregations & Business Metrics
Execute groupings to compute average prices and inventory volumes:

```python
from pyspark.sql.functions import avg, sum, col, round

df_resumo = df_transformado.groupBy("nome") \
    .agg(
        round(avg(col("preco").cast("double")), 2).alias("preco_medio"),
        sum("quantidade").alias("total_quantidade")
    )

display(df_resumo)
```

### Step 7: Full Execution Run
1. Click **Run All** at the top of the notebook to verify that all cells execute sequentially without errors.

---

## 🧪 Validation & Acceptance Criteria

To confirm that batch processing succeeded:
1. DataFrame `df` must render all records without corrupted records (`_corrupt_record`).
2. Column `valor_total_estoque` must be numeric and rounded to 2 decimal places.
3. Summary DataFrame `df_resumo` must produce distinct product groupings with calculated metrics (`preco_medio` and `total_quantidade`).

---

## 🧹 Environment Cleanup

After completing the lab and exporting your notebook, clean up cloud resources:
1. **Delete Volume:** Navigate to **Catalog** -> `workspace` -> `default` -> delete Volume **`teste`**.
2. **Delete Notebook:** Go to **Workspace** -> `Users` -> delete the exercise notebook.

---

## 💡 Advanced Challenges

* **Production-Grade Explicit Schema:** Replace runtime inference (`inferSchema: true`) with an explicit `StructType` and `StructField` definition to eliminate scanning overhead on large datasets.
* **Columnar Parquet Export:** Add a final cell persisting `df_transformado` to columnar Parquet partitioned by product name:
  ```python
  df_transformado.write.mode("overwrite").partitionBy("nome").parquet("/Volumes/workspace/default/teste/output_parquet/")
  ```
