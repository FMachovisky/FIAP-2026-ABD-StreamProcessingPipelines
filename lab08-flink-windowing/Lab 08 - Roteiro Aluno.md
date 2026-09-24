# Lab 08 - Janelas de Tempo e Agregações com Apache Flink SQL

**Curso / Disciplina:** MBA em Engenharia de Dados (ABD) — Stream Processing Pipelines (SPP)  
**Ambiente:** Confluent Cloud for Apache Flink ([confluent.cloud](https://confluent.cloud/))  
**Linguagem / Stack:** Flink SQL / Apache Kafka / Serverless Compute Pool  
**Duração Estimada:** 25 a 30 minutos  

---

## 🎯 Objetivo do Lab

Neste laboratório, você irá dominar um dos pilares mais fundamentais do processamento de fluxo em tempo real: **Janelamento Temporal (*Windowing*)** e agregações com estado (*Stateful Stream Processing*) no **Confluent Cloud for Apache Flink**.

Ao final deste exercício, você será capaz de:
1. Compreender a semântica comparativa entre **Tumbling Windows** (Janelas Fixas/Não sobrepostas) e **Hop Windows** (Janelas Deslizantes).
2. Executar funções de agregação temporal contínua (`COUNT`, `SUM`, `AVG`, `ROUND`) sobre streams infinitos.
3. Observar na prática como a emissão e fechamento de janelas são orquestrados pelos **Watermarks**.
4. Compreender a gestão de memória de estado (*State Backends*) e purga de estado (*State Eviction*) no Flink.

---

## 📋 Pré-requisitos & Materiais

* Acesso ativo ao **Confluent Cloud** ([https://confluent.cloud](https://confluent.cloud)).
* Cluster Kafka e **Flink Compute Pool** provisionados e ativos (do Lab 07).
* SQL Workspace do Confluent Cloud.

---

## 🚀 Passo a Passo Guiado

### Passo 1: Criação da Tabela Geradora (DDL)
No SQL Workspace, execute a instrução DDL criando uma fonte contínua de transações simuladas (5 registros/segundo):

```sql
-- 1. Criação da fonte de transações com conector faker (5 eventos/segundo)
CREATE TABLE transactions_stream (
    transaction_id BIGINT,
    amount DOUBLE,
    transaction_time TIMESTAMP(3),
    -- Watermark: tolerância de 5 segundos a eventos atrasados
    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'faker',
    'rows-per-second' = '5',
    'fields.transaction_id.expression' = '#{number.numberBetween ''1'',''1000''}',
    'fields.amount.expression' = '#{number.randomDouble ''2'',''10'',''1000''}',
    'fields.transaction_time.expression' = '#{date.past ''10'',''SECONDS''}'
);
```

### Passo 2: Agregação em Janela Fixa (Tumbling Window)
As janelas **Tumbling** particionam o fluxo em blocos contíguos e não sobrepostos. Execute no editor:

```sql
-- 2. Agregação em Janela Fixa (Tumbling Window de 1 minuto)
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
> **Dinâmica de Event Time:** O Flink só fecha o cálculo e emite a linha consolidada quando o tempo do sistema (controlado pelo `WATERMARK`) ultrapassa o `window_end`.

### Passo 3: Janela Tumbling Acelerada (10 Segundos)
Para validar o comportamento com menor latência de inspeção, reduza o intervalo:

```sql
-- 3. Tumbling Window rápida de 10 segundos
SELECT 
    window_start, 
    window_end, 
    COUNT(transaction_id) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_volume_brl
FROM TABLE(
    TUMBLE(TABLE transactions_stream, DESCRIPTOR(transaction_time), INTERVAL '10' SECONDS))
GROUP BY window_start, window_end;
```

### Passo 4: Janelas Deslizantes (Hop / Sliding Windows)
As janelas **Hop** recalculam agregados de uma janela maior com um passo de avanço menor:

```sql
-- 4. Janela de 1 minuto recalculada a cada 20 segundos
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

## 🧪 Validação & Critérios de Aceite

Para certificar que o janelamento operou com precisão:
1. Na Tumbling Window, novas linhas consolidadas devem ser emitidas a cada fechamento de intervalo sem duplicar registros entre intervalos contíguos.
2. Na Hop Window, cada registro deve contribuir para múltiplos intervalos vizinhos espaçados pelo slide de 20 segundos.
3. As colunas `window_start` e `window_end` devem demarcar com exatidão a janela temporal de agregação.

---

## 🧹 Cleanup (Limpeza do Ambiente)

1. **Parar Queries Ativas:** No SQL Workspace, interrompa a execução da query de agregação clicando em **Stop**.
2. **Remover a Tabela:**
   ```sql
   DROP TABLE IF EXISTS transactions_stream;
   ```
3. **Preservar o Cluster:** Mantenha o Cluster Kafka ativo para o Lab 09.

---

## 💡 Desafios Complementares

1. **Cumulate Windows (Janelas Cumulativas):** Pesquise e teste a função de janela `CUMULATE` do Flink SQL para consolidar métricas diárias acumuladas com emissões parciais a cada 10 segundos.
2. **Agrupamento por Dimensão Dinâmica:** Adicione uma coluna simulada de categoria (`status_pagamento`) e agrupe a janela por `window_start, window_end, status_pagamento`.
