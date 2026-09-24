# Lab 09 - Real-Time Stream Joins and Enrichment with Flink SQL

**Course / Track:** MBA in Data Engineering (ABD) — Stream Processing & Pipelines (SPP)  
**Environment:** Confluent Cloud for Apache Flink ([confluent.cloud](https://confluent.cloud/))  
**Language / Stack:** Flink SQL / Apache Kafka / Serverless Compute Pool  
**Estimated Duration:** 25 to 30 minutes  

---

## 🎯 Lab Objectives

In this laboratory, you will learn how to execute continuous relational join operations (**Stream Joins**) in real time on **Confluent Cloud for Apache Flink**, enriching high-frequency transient telemetry with dimensional and master data.

By the end of this lab, you will be able to:
1. Register multiple streaming event generator tables and reference dimension tables in the Flink catalog.
2. Execute continuous relational `LEFT JOIN` queries correlating event transactions with user identifiers.
3. Understand bi-directional state management (*Stateful Joins*) and the formal semantics of *Dynamic Tables*.
4. Apply analytical conditional expressions (`CASE WHEN`) directly over continuous output streams.
5. Perform complete resource teardown and serverless cluster decommission.

---

## 📋 Prerequisites & Materials

* Active account on **Confluent Cloud** ([https://confluent.cloud](https://confluent.cloud)).
* Active Kafka cluster and **Flink Compute Pool** provisioned and running.
* Confluent Cloud SQL Workspace.

---

## 🚀 Step-by-Step Guide

### Step 1: Create Streaming Transactions Table (DDL 1)
In the SQL Workspace query editor, register the financial transaction stream:

```sql
-- 1. Real-time transaction event stream with watermark
CREATE TABLE transactions_join_stream (
    transaction_id BIGINT,
    user_id INT,
    amount DOUBLE,
    transaction_time TIMESTAMP(3),
    -- Watermark: 5-second out-of-order tolerance
    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'faker',
    'rows-per-second' = '2',
    'fields.user_id.expression' = '#{number.numberBetween ''1'',''10''}',
    'fields.amount.expression' = '#{number.randomDouble ''2'',''10'',''1000''}',
    'fields.transaction_time.expression' = '#{date.past ''10'',''SECONDS''}'
);
```

### Step 2: Create Reference Dimension Users Table (DDL 2)
In a new query cell, register the reference user dimension:

```sql
-- 2. Reference user dimension table
CREATE TABLE users_reference (
    user_id INT,
    user_name STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'faker',
    'fields.user_id.expression' = '#{number.numberBetween ''1'',''10''}',
    'fields.user_name.expression' = '#{Name.firstName}'
);
```

### Step 3: Execute Continuous Real-Time Enrichment Query
Launch the continuous join query applying loyalty tier classifications:

```sql
-- 3. Continuous stream enrichment query with dynamic loyalty categorization
SELECT 
    t.transaction_id,
    t.amount,
    u.user_name,
    CASE 
        WHEN t.user_id IN (1, 4, 8) THEN 'PLATINUM'
        WHEN t.user_id IN (2, 6, 10) THEN 'GOLD'
        WHEN t.user_id IN (3, 7) THEN 'SILVER'
        ELSE 'BRONZE'
    END AS loyalty_level,
    t.transaction_time
FROM transactions_join_stream t
LEFT JOIN users_reference u ON t.user_id = u.user_id;
```

Inspect the **Results** panel as each incoming transaction is instantaneously decorated with the customer's name and loyalty category.

### Step 4: Targeted Stream Filtering (VIP Transactions)
Isolate and stream transactions belonging strictly to `PLATINUM` tier customers:

```sql
-- 4. Filtered continuous monitoring for PLATINUM customers
SELECT 
    t.transaction_id,
    t.amount,
    u.user_name,
    'PLATINUM' AS loyalty_level,
    t.transaction_time
FROM transactions_join_stream t
LEFT JOIN users_reference u ON t.user_id = u.user_id
WHERE t.user_id IN (1, 4, 8);
```

---

## 🧪 Validation & Acceptance Criteria

To confirm that stream joining functioned properly:
1. Output records emitted in the Results panel must feature both original event attributes (`amount`, `transaction_id`) and joined dimension attributes (`user_name`, `loyalty_level`).
2. Targeted VIP filtering must restrict stream emissions strictly to user IDs configured in `IN (1, 4, 8)`.
3. The continuous query must run without cardinality faults or key mismatch dropouts.

---

## 🧹 Environment Cleanup

1. **Stop Active Stream Queries:** Click **Stop** on running query tabs.
2. **Drop Catalog Tables:**
   ```sql
   DROP TABLE IF EXISTS transactions_join_stream;
   DROP TABLE IF EXISTS users_reference;
   ```
3. **Confluent Cloud Teardown:**
   * If you are not utilizing the Kafka cluster for other personal projects, go to **Cluster Settings** -> **Delete Cluster** and delete the environment to prevent any residual credit consumption.

---

## 💡 Advanced Challenges

1. **Interval Joins (Time-Bounded Correlated Joins):** Constrain the join condition with an explicit temporal boundary:
   ```sql
   WHERE t.transaction_time BETWEEN u.update_time - INTERVAL '2' MINUTE AND u.update_time
   ```
2. **External Lookup Joins:** Research how the Flink JDBC / PostgreSQL connector executes point-in-time *Lookup Joins* against enterprise relational databases in high-throughput production topologies.
