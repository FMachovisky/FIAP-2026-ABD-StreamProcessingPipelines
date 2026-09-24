# Lab 04 - Janelamento Avançado e Watermarking no Apache Spark Streaming

**Curso / Disciplina:** MBA em Engenharia de Dados (ABD) — Stream Processing Pipelines (SPP)  
**Ambiente:** Databricks Free Edition ([login.databricks.com](https://login.databricks.com/))  
**Linguagem / Stack:** Python 3.11+ / PySpark Structured Streaming / Spark SQL  
**Duração Estimada:** 25 a 30 minutos  

---

## 🎯 Objetivo do Lab

Neste laboratório, você irá aprender a lidar com agregação temporal avançada e dados tardios (*late data*) utilizando **Janelas Deslizantes (*Sliding Windows*)** e a técnica de **Watermarking** no Apache Spark Streaming.

Ao final deste exercício, você será capaz de:
1. Configurar colunas temporais com `TimestampType` para habilitar processamento baseado em tempo de evento (*Event Time*).
2. Compreender a diferença prática e arquitetural entre **Janelas Fixas (*Tumbling*)** e **Janelas Deslizantes (*Sliding*)**.
3. Implementar a cláusula `withWatermark()` para gerenciar o acúmulo de estado em memória e descartar eventos obsoletos.
4. Identificar a sobreposição de eventos entre janelas vizinhas geradas pelo intervalo de deslizamento (*slide duration*).
5. Consultar e ordenar os intervalos temporais (`window.start` e `window.end`) via **Spark SQL**.

---

## 📋 Pré-requisitos & Materiais

* Acesso ativo ao **Databricks Free Edition** ([login.databricks.com](https://login.databricks.com/)).
* Cluster de computação ativo no workspace.
* Volume **`checkpoint`** criado no catálogo `workspace` / schema `default`.
* Dataset de eventos embutido no Databricks: `/databricks-datasets/structured-streaming/events/`
* Notebook de execução: [`Lab 04 - Janelamento Avancado e Watermarking no Apache Spark Streaming.ipynb`](./Lab%2004%20-%20Janelamento%20Avancado%20e%20Watermarking%20no%20Apache%20Spark%20Streaming.ipynb)

---

## 🚀 Passo a Passo Guiado

### Passo 1: Acesso e Importação do Notebook
1. Acesse o Databricks em [https://login.databricks.com/](https://login.databricks.com/).
2. No menu lateral, acesse **Workspace** -> **Users** -> seu e-mail de usuário.
3. Importe o arquivo `Lab 04 - Janelamento Avancado e Watermarking no Apache Spark Streaming.ipynb` arrastando e soltando (**Drag & Drop**).
4. Associe o notebook ao seu cluster ativo.

### Passo 2: Schema Explícito com Timestamp de Evento
Para aplicar janelamento temporal, é mandatória a tipagem da coluna de tempo como `TimestampType`:
```python
from pyspark.sql.types import StructType, StructField, TimestampType, StringType
from pyspark.sql.functions import col, window

inputPath = "/databricks-datasets/structured-streaming/events/"

# 1. Definição explícita do Schema
jsonSchema = StructType([
    StructField("time", TimestampType(), True),
    StructField("action", StringType(), True)
])

# 2. Leitura contínua emulando tempo real (1 arquivo por micro-batch)
df_streaming = (
    spark.readStream
        .schema(jsonSchema)
        .option("maxFilesPerTrigger", 1)
        .json(inputPath)
)
```

### Passo 3: Aplicação de Watermarking e Janelamento Deslizante
Configuramos uma tolerância de 10 minutos para eventos que chegam fora de ordem e uma janela de 10 minutos que desliza a cada 5 minutos:
```python
# 3. Watermark de 10 minutos + Janela deslizante (Tamanho: 10 min, Slide: 5 min)
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
> **Conceito Chave:** Como o *slide* (5 min) é menor que a duração da janela (10 min), os registros intermediários serão contabilizados em **duas janelas consecutivas** (sobreposição temporal).

### Passo 4: Inicialização da Query com Limpeza de Checkpoint
Configuramos o sink em memória e limpamos o diretório de checkpoint para permitir reexecuções controladas:
```python
# Parar eventuais queries ativas anteriores
for stream in spark.streams.active:
    stream.stop()

# Limpeza do diretório de checkpoint específico do Lab 04
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
> **Tolerância a Falhas e Gestão de Estado:** O Watermark define o limiar a partir do qual o Spark descarta o histórico antigo da memória de estado (`max(eventTime) - watermarkDelay`). Sem Watermarking, uma query com janelas acumularia dados de estado indefinidamente até causar erro de *Out of Memory (OOM)*.

### Passo 5: Consulta e Visualização dos Intervalos de Janela em SQL
Consulte a tabela `contagem_janelada` desmembrando os limites inferior e superior de cada janela:
```sql
%sql
SELECT 
    window.start AS inicio_janela,
    window.end AS fim_janela,
    action,
    count AS total_eventos
FROM contagem_janelada
ORDER BY inicio_janela DESC, action
```

### Passo 6: Encerramento Gracioso da Query
Certifique-se de finalizar a query ativa antes de avançar para a próxima prática:
```python
# Verificar status e parar
print(f"Query ativa: {query.isActive}")
query.stop()
```

---

## 🧪 Validação & Critérios de Aceite

Para certificar que o janelamento e o watermarking funcionaram corretamente:
1. A consulta SQL deve retornar pares de colunas `inicio_janela` e `fim_janela` espaçados pelo tamanho da janela (10 minutos) e deslocados pelo passo de slide (5 minutos).
2. Eventos ocorridos em intervalos sobrepostos devem ser contabilizados em mais de uma janela adjacente.
3. A telemetria do micro-batch em `query.lastProgress` deve confirmar a presença e o avanço da chave `watermark` em `eventTime`.

---

## 🧹 Cleanup (Limpeza do Ambiente)

1. **Apagar Checkpoints Antigos:** Excluir os diretórios dentro do Volume `checkpoint`.
2. **Apagar o Notebook:** No menu **Workspace** -> `Users` -> remover o notebook do Lab 04 se necessário.

---

## 💡 Desafios Complementares

1. **Comparação com Janela Fixa (*Tumbling Window*):** Altere a função de janelamento para remover o parâmetro de deslizamento:
   ```python
   df_windowed = (
       df_streaming
           .withWatermark("time", "10 minutes")
           .groupBy(
               window(col("time"), "10 minutes"), # Janela fixa de 10 min
               col("action")
           )
           .count()
   )
   ```
   *Execute novamente e observe como os intervalos tornam-se contíguos (`15:00-15:10`, `15:10-15:20`), eliminando a sobreposição de contagem entre janelas.*

2. **Métricas de Água (Watermark Progression):** Execute `display(query.lastProgress)` no Python e localize o campo `eventTime` para acompanhar o avanço da marca d'água calculada pelo Spark a cada micro-batch.
