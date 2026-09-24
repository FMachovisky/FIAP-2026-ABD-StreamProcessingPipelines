# Lab 09 - Joins e Enriquecimento de Streams em Flink SQL

**Curso / Disciplina:** MBA em Engenharia de Dados (ABD) — Stream Processing & Pipelines (SPP)  
**Ambiente:** Confluent Cloud for Apache Flink ([confluent.cloud](https://confluent.cloud/))  
**Linguagem / Stack:** Flink SQL / Apache Kafka / Serverless Compute Pool  
**Duração Estimada:** 25 a 30 minutos  

---

## 🎯 Objetivo do Lab

Neste laboratório, você aprenderá a executar operações relacionais contínuas de cruzamento (**Stream Joins**) em tempo real no **Confluent Cloud for Apache Flink**, enriquecendo fluxos de alta frequência com dados cadastrais e dimensionais.

Ao final deste exercício, você será capaz de:
1. Criar e gerenciar múltiplas tabelas geradoras de stream e tabelas dimensionais no catálogo Flink.
2. Executar cruzamentos relacionais contínuos (`LEFT JOIN`) correlacionando identificadores de transação e chaves de cliente.
3. Compreender a gestão de estado bi-direcional (*Stateful Joins*) e a semântica de *Dynamic Tables*.
4. Aplicar transformações condicionais analíticas (`CASE WHEN`) diretamente sobre o fluxo contínuo resultante.
5. Operar a limpeza completa de recursos e encerramento de clusters serverless.

---

## 📋 Pré-requisitos & Materiais

* Acesso ativo ao **Confluent Cloud** ([https://confluent.cloud](https://confluent.cloud)).
* Cluster Kafka e **Flink Compute Pool** provisionados e ativos.
* SQL Workspace do Confluent Cloud.

---

## 🚀 Passo a Passo Guiado

### Passo 1: Criação da Tabela de Transações (DDL 1)
No SQL Workspace, abra uma aba de consulta e registre o stream de transações financeiras:

```sql
-- 1. Stream de eventos de transações com Watermark
CREATE TABLE transactions_join_stream (
    transaction_id BIGINT,
    user_id INT,
    amount DOUBLE,
    transaction_time TIMESTAMP(3),
    -- Watermark: tolerância de até 5 segundos a atrasos
    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'faker',
    'rows-per-second' = '2',
    'fields.user_id.expression' = '#{number.numberBetween ''1'',''10''}',
    'fields.amount.expression' = '#{number.randomDouble ''2'',''10'',''1000''}',
    'fields.transaction_time.expression' = '#{date.past ''10'',''SECONDS''}'
);
```

### Passo 2: Criação da Tabela Dimensional de Usuários (DDL 2)
Em uma nova célula, registre a tabela dimensional de usuários de referência:

```sql
-- 2. Tabela de referência de usuários
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

### Passo 3: Executando o Enriquecimento em Tempo Real (Continuous Query)
Execute a consulta contínua com lógica de fidelidade:

```sql
-- 3. Query contínua de enriquecimento com categorização dinâmica
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

Acompanhe na aba **Results** cada transação recebida sendo enriquecida instantaneamente com o nome do cliente e o nível de fidelidade.

### Passo 4: Filtragem Direcionada (Clientes VIP)
Isole transações exclusivas de clientes da categoria `PLATINUM`:

```sql
-- 4. Monitoramento direcionado para clientes PLATINUM
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

## 🧪 Validação & Critérios de Aceite

Para certificar que o join de streams operou com sucesso:
1. O painel Results deve emitir linhas contínuas onde tanto os campos da transação (`amount`, `transaction_id`) quanto os da dimensão (`user_name`, `loyalty_level`) estão preenchidos.
2. A filtragem de clientes VIP deve restringir a emissão exclusivamente a registros correspondentes aos identificadores definidos na cláusula `IN (1, 4, 8)`.
3. Não devem ocorrer erros de cardinalidade ou quebra de chave relacional durante a ingestão contínua.

---

## 🧹 Cleanup (Limpeza do Ambiente)

1. **Parar Queries Ativas:** No SQL Workspace, interrompa a execução da query contínua clicando em **Stop**.
2. **Remover as Tabelas do Catálogo:**
   ```sql
   DROP TABLE IF EXISTS transactions_join_stream;
   DROP TABLE IF EXISTS users_reference;
   ```
3. **Encerramento da Infraestrutura Confluent Cloud:**
   * Caso não utilize o cluster para outros estudos, acesse **Cluster Settings** -> **Delete Cluster** para cessar completamente a alocação de créditos da conta.

---

## 💡 Desafios Complementares

1. **Interval Joins (Janela de Tolerância Relacional):** Adapte o join para exigir que o evento de dimensão ocorra em uma janela temporal delimitada:
   ```sql
   WHERE t.transaction_time BETWEEN u.update_time - INTERVAL '2' MINUTE AND u.update_time
   ```
2. **Lookup Joins Externos:** Pesquise como o conector JDBC / PostgreSQL do Flink permite executar *Lookup Joins* diretamente contra bancos de dados relacionais transacionais corporativos.
