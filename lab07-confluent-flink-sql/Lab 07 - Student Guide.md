# Lab 07 - Introduction to Confluent Cloud and Flink SQL Hello World

**Course / Track:** MBA in Data Engineering (ABD) — Stream Processing Pipelines (SPP)  
**Environment:** Confluent Cloud for Apache Flink ([confluent.cloud](https://confluent.cloud/))  
**Language / Stack:** Flink SQL / Apache Kafka / Serverless Compute Pool  
**Estimated Duration:** 25 to 30 minutes  

---

## 🎯 Lab Objectives

In this laboratory, you will begin your hands-on journey with native stream processing using **Apache Flink**, deployed on the managed serverless infrastructure of **Confluent Cloud for Apache Flink**.

By the end of this lab, you will be able to:
1. Create and configure a cloud environment and Kafka cluster on Confluent Cloud.
2. Provision a serverless **Flink Compute Pool** colocated in the same cloud region.
3. Execute DDL instructions in the Flink SQL Workspace to register synthetic event streams using the `faker` connector.
4. Configure declarative **Event Time** and **Watermarking** in SQL.
5. Launch and monitor a **Continuous Query** (`SELECT *`) consuming real-time event streams.
6. Manage operational costs and resource governance (*CFUs* and query lifecycle).

---

## 📋 Prerequisites & Materials

* Internet access and a modern web browser.
* Free registration on **Confluent Cloud** ([https://confluent.cloud/signup](https://confluent.cloud/signup)) with $400 in trial credits.
* Access to Confluent Cloud SQL Workspace.

---

## 🚀 Step-by-Step Guide

### Step 1: Confluent Cloud Account Registration
1. Visit the signup portal: [https://confluent.cloud/signup](https://confluent.cloud/signup).
2. Enter your details or sign in via OAuth (Google/GitHub).
3. Confirm the verification email to unlock **$400 in free trial credits** (valid for 30 days).

> [!NOTE]
> **Cost Management:** Trial credits are generous and cover all course labs. Stop continuous queries when not actively analyzing results to preserve capacity.

### Step 2: Environment and Kafka Cluster Setup
1. In the **Cloud Console**, select environment `default` or create a dedicated environment (e.g. `fiap-abd-env`).
2. Click **Create Cluster**:
   * Cluster type: **Basic** (optimal for development and learning workloads).
   * Cloud Provider & Region: **AWS** in `us-east-1` (N. Virginia) or `us-east-2` (Ohio).
   * Cluster Name: `cluster-spp-abd`.
   * Click **Launch Cluster**.

### Step 3: Provision Flink Compute Pool
In Confluent Cloud, distributed Flink engines are operated through **SQL Workspaces**:
1. In the left navigation sidebar, click **`SQL workspaces`**.
2. Select your Compute Pool dropdown or click **"Create compute pool"**:
   * **Provider & Region:** Match the **exact region** where the Kafka cluster resides (e.g. `AWS / us-east-1`).
   * **Pool Name:** `flink-pool-abd`.
   * **Maximum Capacity (CFU):** Keep the suggested default (e.g. `5` or `10` CFUs). In serverless Flink, billing is strictly metered per execution second.
3. Wait until the Compute Pool status indicates **Active**.

### Step 4: Synthetic Event Stream DDL (Faker Connector)
In the SQL Workspace query editor, paste the DDL instruction and click **Run**:

```sql
-- 1. Create synthetic transaction generator table with official 'faker' connector
CREATE TABLE transactions (
    transaction_id BIGINT,
    amount DOUBLE,
    transaction_time TIMESTAMP(3),
    -- Watermark: define 5-second tolerance for out-of-order events
    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'faker',
    'rows-per-second' = '2',
    'fields.transaction_id.expression' = '#{number.numberBetween ''1'',''1000''}',
    'fields.amount.expression' = '#{number.randomDouble ''2'',''1'',''500''}',
    'fields.transaction_time.expression' = '#{date.past ''10'',''SECONDS''}'
);
```

### Step 5: Execute Continuous Streaming Query
Open a new query tab or clear the editor and run the continuous stream query:

```sql
-- 2. Continuous Query consuming streaming events in real time
SELECT 
    transaction_id,
    amount,
    transaction_time
FROM transactions;
```

Inspect the **Results** panel as synthetic transactions stream in continuously.

---

## 🧪 Validation & Acceptance Criteria

To confirm that the Flink SQL runtime is operating properly:
1. The Flink Compute Pool must show status `Active` without regional provisioning alerts.
2. The DDL statement `CREATE TABLE transactions` must successfully register within the catalog.
3. The Results panel must output streaming rows at the configured 2 records/second rate.

---

## 🧹 Environment Cleanup

1. **Stop Continuous Query:** Click **Stop** on the running query tab in the SQL Workspace.
2. **Drop Generator Table:**
   ```sql
   DROP TABLE IF EXISTS transactions;
   ```
3. **Preserve Kafka Cluster:** Keep the Kafka cluster intact for Labs 08 and 09.

---

## 💡 Advanced Challenges

1. **Throughput Scaling:** Modify `'rows-per-second' = '10'` in the DDL and observe the higher throughput stream in the Results panel.
2. **Inline Fraud Filter:** Run a streaming query isolating high-value outlier transactions:
   ```sql
   SELECT * FROM transactions WHERE amount > 400.00;
   ```
