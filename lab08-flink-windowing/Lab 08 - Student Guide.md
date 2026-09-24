# Lab 08 - Temporal Windowing and Aggregations with Apache Flink SQL

**Course / Track:** MBA in Data Engineering (ABD) — Stream Processing Pipelines (SPP)  
**Environment:** Confluent Cloud for Apache Flink ([confluent.cloud](https://confluent.cloud/))  
**Language / Stack:** Flink SQL / Apache Kafka / Serverless Compute Pool  
**Estimated Duration:** 25 to 30 minutes  

---

## 🎯 Lab Objectives

In this laboratory, you will master one of the foundational pillars of real-time stream processing: **Temporal Windowing** and **Stateful Stream Processing** using **Confluent Cloud for Apache Flink**.

By the end of this lab, you will be able to:
1. Contrast the operational semantics of **Tumbling Windows** (fixed, non-overlapping) and **Hop Windows** (sliding, overlapping).
2. Execute continuous temporal aggregation functions (`COUNT`, `SUM`, `AVG`, `ROUND`) over unbounded streaming datasets.
3. Observe how window emission and closing triggers are orchestrated by declarative **Watermarks**.
4. Understand state store lifecycles (*State Backends*) and automated state eviction upon window finalization.

---

## 📋 Prerequisites & Materials

* Active account on **Confluent Cloud** ([https://confluent.cloud](https://confluent.cloud)).
* Active Kafka cluster and **Flink Compute Pool** provisioned in Lab 07.
* Confluent Cloud SQL Workspace.

---

## 🚀 Step-by-Step Guide

### Step 1: Create Transaction Generator Table (DDL)
In the SQL Workspace, execute the DDL creating an event generator producing 5 records/second:

```sql
-- 1. Create streaming synthetic transactions table with faker connector
CREATE TABLE transactions_stream (
    transaction_id BIGINT,
    amount DOUBLE,
    transaction_time TIMESTAMP(3),
    -- Watermark: 5-second tolerance for out-of-order records
    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'faker',
    'rows-per-second' = '5',
    'fields.transaction_id.expression' = '#{number.numberBetween ''1'',''1000''}',
    'fields.amount.expression' = '#{number.randomDouble ''2'',''10'',''1000''}',
    'fields.transaction_time.expression' = '#{date.past ''10'',''SECONDS''}'
);
```

### Step 2: Fixed Window Aggregations (Tumbling Window)
Tumbling windows partition continuous time into distinct, non-overlapping slices. Run in the editor:

```sql
-- 2. Tumbling Window aggregation (1-minute fixed window)
SELECT 
    window_start, 
    window_end, 
    COUNT(transaction_id) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_volume_brl,
    ROUND(AVG(amount), 2) AS avg_ticket_brl
FROM TABLE(
    TUMBLE(TABLE transactions_stream, DESCRIPTOR(transaction_time), INTERVAL '1' MINUTES))
GROUP BY window_start, window_end;
```

> [!NOTE]
> **Event Time Dynamics:** Flink evaluates and closes the aggregation window only after the system's watermark surpasses `window_end`.

### Step 3: Fast Tumbling Window (10 Seconds)
For accelerated feedback and reduced observation latency:

```sql
-- 3. Rapid 10-second Tumbling Window
SELECT 
    window_start, 
    window_end, 
    COUNT(transaction_id) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_volume_brl
FROM TABLE(
    TUMBLE(TABLE transactions_stream, DESCRIPTOR(transaction_time), INTERVAL '10' SECONDS))
GROUP BY window_start, window_end;
```

### Step 4: Sliding Window Aggregations (Hop Windows)
Hop windows recalculate aggregated metrics over longer intervals with shorter slide steps:

```sql
-- 4. 1-minute window recomputed every 20 seconds
SELECT 
    window_start, 
    window_end, 
    COUNT(transaction_id) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_volume_brl
FROM TABLE(
    HOP(TABLE transactions_stream, DESCRIPTOR(transaction_time), INTERVAL '20' SECONDS, INTERVAL '1' MINUTES))
GROUP BY window_start, window_end;
```

---

## 🧪 Validation & Acceptance Criteria

To confirm that temporal windowing operated as expected:
1. Tumbling Window results must produce sequential rows without duplicate counts across adjacent intervals.
2. Hop Window results must demonstrate that individual transactions contribute to multiple overlapping window slices.
3. Columns `window_start` and `window_end` must match expected temporal bounds exactly.

---

## 🧹 Environment Cleanup

1. **Stop Active Stream Queries:** Click **Stop** on running SQL tabs.
2. **Drop Generator Table:**
   ```sql
   DROP TABLE IF EXISTS transactions_stream;
   ```
3. **Preserve Kafka Cluster:** Keep the Kafka cluster intact for Lab 09.

---

## 💡 Advanced Challenges

1. **Cumulative Windows (`CUMULATE`):** Explore Flink SQL's `CUMULATE` window function to model cumulative daily volume with periodic early-trigger emissions.
2. **Multi-Dimensional Windowing:** Add a synthetic status dimension (`status_pagamento`) and group by `window_start, window_end, status_pagamento`.
