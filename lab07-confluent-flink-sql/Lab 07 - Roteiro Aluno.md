# Lab 07 - Introdução ao Confluent Cloud e Flink SQL Hello World

**Curso / Disciplina:** MBA em Engenharia de Dados (ABD) — Stream Processing & Pipelines (SPP)  
**Ambiente:** Confluent Cloud for Apache Flink ([confluent.cloud](https://confluent.cloud/))  
**Linguagem / Stack:** Flink SQL / Apache Kafka / Serverless Compute Pool  
**Duração Estimada:** 25 a 30 minutos  

---

## 🎯 Objetivo do Lab

Neste laboratório, você iniciará a jornada no processamento de streams nativo com **Apache Flink**, utilizando a infraestrutura gerenciada e *serverless* do **Confluent Cloud for Apache Flink**.

Ao final deste exercício, você será capaz de:
1. Criar e configurar um ambiente de nuvem e cluster Kafka no Confluent Cloud.
2. Provisionar um **Flink Compute Pool** serverless na mesma região do cluster.
3. Executar instruções DDL no Flink SQL Workspace para registrar tabelas de eventos sintéticos utilizando o conector `faker`.
4. Configurar a semântica de **Event Time** e **Watermarking** declarativo em SQL.
5. Iniciar e monitorar uma **Continuous Query** (`SELECT *`) consumindo fluxos em tempo real.
6. Gerenciar custos e governança operacional (*CFUs* e encerramento de queries).

---

## 📋 Pré-requisitos & Materiais

* Acesso à internet e navegador moderno.
* Cadastro gratuito no **Confluent Cloud** ([https://confluent.cloud/signup](https://confluent.cloud/signup)) com US$ 400 em créditos de avaliação.
* SQL Workspace do Confluent Cloud.

---

## 🚀 Passo a Passo Guiado

### Passo 1: Cadastro no Confluent Cloud
1. Acesse o portal de cadastro: [https://confluent.cloud/signup](https://confluent.cloud/signup).
2. Preencha seus dados corporativos/acadêmicos ou utilize login social (Google/GitHub).
3. Confirme o e-mail de ativação para liberar os **US$ 400 em créditos gratuitos** (válidos por 30 dias).

> [!NOTE]
> **Gestão de Custos:** Os créditos cobrem amplamente todos os laboratórios. Ao término de cada sessão, pare as queries contínuas para evitar consumo residual da sua cota.

### Passo 2: Configuração de Ambiente e Cluster Kafka
1. No painel principal (**Cloud Console**), selecione o ambiente padrão (`default`) ou crie um novo (ex: `fiap-abd-env`).
2. Clique em **Create Cluster**:
   * Selecione o tipo de cluster: **Basic** (ideal para desenvolvimento e fins acadêmicos).
   * Provedor de Nuvem & Região: **AWS** na região `us-east-1` (N. Virginia) ou `us-east-2` (Ohio).
   * Nome do Cluster: `cluster-spp-abd`.
   * Clique em **Launch Cluster**.

### Passo 3: Provisionamento do Flink Compute Pool
No Confluent Cloud, a engine distribuída de Flink é operada via **SQL Workspaces**:
1. No menu lateral esquerdo, clique em **`SQL workspaces`**.
2. No topo da tela, clique no seletor de Compute Pool ou em **"Create compute pool"**:
   * **Provedor e Região:** Selecione **exatamente a mesma região** do Cluster Kafka (ex: `AWS / us-east-1`).
   * **Nome do Pool:** `flink-pool-abd`.
   * **Capacidade Máxima (CFU):** Mantenha o padrão sugerido (ex: `5` ou `10` CFUs). O faturamento é proporcional aos segundos de computação ativa.
3. Aguarde até que o status do Compute Pool mude para **Active**.

### Passo 4: DDL da Tabela Geradora (Faker Connector)
No editor do SQL Workspace, cole a instrução DDL e clique em **Run**:

```sql
-- 1. Criação da tabela geradora de eventos usando o conector oficial 'faker'
CREATE TABLE transactions (
    transaction_id BIGINT,
    amount DOUBLE,
    transaction_time TIMESTAMP(3),
    -- Watermark: define tolerância de até 5 segundos para eventos atrasados
    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'faker',
    'rows-per-second' = '2',
    'fields.transaction_id.expression' = '#{number.numberBetween ''1'',''1000''}',
    'fields.amount.expression' = '#{number.randomDouble ''2'',''1'',''500''}',
    'fields.transaction_time.expression' = '#{date.past ''10'',''SECONDS''}'
);
```

### Passo 5: Execução da Consulta Contínua (Continuous Query)
Em uma nova célula de consulta, execute a leitura contínua:

```sql
-- 2. Continuous Query consumindo os eventos em tempo real
SELECT 
    transaction_id,
    amount,
    transaction_time
FROM transactions;
```

Acompanhe no painel inferior **Results** os registros sendo emitidos continuamente a cada segundo.

---

## 🧪 Validação & Critérios de Aceite

Para certificar a prontidão do ambiente Flink SQL:
1. O Compute Pool deve permanecer no estado `Active` sem falhas de provisionamento regional.
2. A instrução DDL `CREATE TABLE transactions` deve retornar confirmação de registro no catálogo de metadados.
3. O painel inferior do SQL Workspace deve emitir registros incrementais com IDs aleatórios e valores decimais simulados.

---

## 🧹 Cleanup (Limpeza do Ambiente)

1. **Parar a Query Contínua:** No SQL Workspace, clique no botão **Stop** da query ativa.
2. **Remover a Tabela de Teste:**
   ```sql
   DROP TABLE IF EXISTS transactions;
   ```
3. **Preservar o Cluster:** Mantenha o Cluster Kafka ativo para uso nos Labs 08 e 09.

---

## 💡 Desafios Complementares

1. **Vazão Customizada:** Altere o parâmetro `'rows-per-second' = '10'` no DDL e observe o aumento imediato na taxa de ingestão de eventos no painel de resultados.
2. **Filtro de Fraude Inline:** Execute uma query contínua filtrando transações atípicas de alto valor:
   ```sql
   SELECT * FROM transactions WHERE amount > 400.00;
   ```
