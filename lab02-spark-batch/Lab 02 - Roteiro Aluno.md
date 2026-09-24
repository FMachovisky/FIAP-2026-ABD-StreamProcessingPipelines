# Lab 02 - Ingestão e Processamento Batch no Apache Spark (PySpark)

**Curso / Disciplina:** MBA em Engenharia de Dados (ABD) — Stream Processing Pipelines (SPP)  
**Ambiente:** Databricks Free Edition ([login.databricks.com](https://login.databricks.com/))  
**Linguagem / Stack:** Python 3.11+ / Apache Spark (PySpark) / DataFrames API  
**Duração Estimada:** 25 a 30 minutos  

---

## 🎯 Objetivo do Lab

Neste laboratório, você irá aprender a realizar a **ingestão e o processamento de dados em modo Batch** utilizando a API de **DataFrames** do PySpark no Databricks.

Ao final deste laboratório, você será capaz de:
1. Provisionar a estrutura de armazenagem (Volume no Unity Catalog) no Databricks.
2. Carregar arquivos JSON estruturados multiline com inferência e validação de schema.
3. Executar transformações relacionais: projeção, filtragem e criação de colunas derivadas.
4. Realizar agregações analíticas e sumarizações de métricas de negócio.
5. Persistir dados em tabelas temporárias/permanentes e validar a integridade dos dados processados.

---

## 📋 Pré-requisitos & Materiais

* Acesso ativo ao **Databricks Free Edition** ([login.databricks.com](https://login.databricks.com/)).
* Arquivo de dados: [`dataset.json`](./dataset.json)
* Notebook de execução: [`Lab 02 - Exemplo de Ingestao e Processamento Batch no Apache Spark.ipynb`](./Lab%2002%20-%20Exemplo%20de%20Ingestao%20e%20Processamento%20Batch%20no%20Apache%20Spark.ipynb)

---

## 🚀 Passo a Passo Guiado

### Passo 1: Acesso e Importação do Notebook
1. Acesse o Databricks em [https://login.databricks.com/](https://login.databricks.com/).
2. No menu lateral esquerdo, acesse **Workspace** -> **Users** -> seu e-mail de usuário.
3. Importe o notebook `Lab 02 - Exemplo de Ingestao e Processamento Batch no Apache Spark.ipynb` utilizando a opção de **Drag & Drop** (arrastar e soltar) na janela de importação.

### Passo 2: Criação do Volume e Upload dos Dados
1. No menu lateral esquerdo, acesse a aba **Catalog**.
2. Navegue até o catálogo **`workspace`** e o schema **`default`**.
3. Crie um novo Volume com o nome **`teste`** (caminho final: `/Volumes/workspace/default/teste/`).
4. Faça o upload do arquivo `dataset.json` arrastando e soltando (**Drag & Drop**) para dentro do Volume `teste`.

### Passo 3: Verificação de Nomes e Caminhos no Notebook
1. Abra o notebook importado e certifique-se de associá-lo a um cluster ativo.
2. Verifique se o caminho do arquivo JSON na célula de ingestão corresponde ao Volume criado:
   ```python
   file_location = "/Volumes/workspace/default/teste/dataset.json"
   file_type = "json"
   ```
3. Valide também os nomes das **tabelas temporárias e permanentes** declaradas nas células subsequentes do notebook.

### Passo 4: Leitura e Ingestão Batch
Execute as células de carga:

```python
# Ingestão em lote com suporte a multiline
df = spark.read.format(file_type) \
    .option("multiline", "true") \
    .option("inferSchema", "true") \
    .load(file_location)

# Exibir os dados e o schema inferido
display(df)
df.printSchema()
```

### Passo 5: Transformações e Filtros
1. **Filtragem de Registros:** Filtrar produtos por faixa de preço (`display(df.filter(df["preco"].cast("double") > 100))`).
2. **Coluna Calculada:** Criar a coluna `valor_total_estoque` multiplicando `preco` por `quantidade`:

```python
from pyspark.sql.functions import col, round

df_transformado = df.withColumn("valor_total_estoque", round(col("preco").cast("double") * col("quantidade"), 2))
display(df_transformado)
```

### Passo 6: Agregações & Métricas de Negócio
Execute os agrupamentos para consolidar valores médios e totais:

```python
from pyspark.sql.functions import avg, sum, col, round

df_resumo = df_transformado.groupBy("nome") \
    .agg(
        round(avg(col("preco").cast("double")), 2).alias("preco_medio"),
        sum("quantidade").alias("total_quantidade")
    )

display(df_resumo)
```

### Passo 7: Execução Completa
1. Clique em **Run All** no topo do notebook para garantir que todas as células foram executadas sequencialmente sem erros.

---

## 🧪 Validação & Critérios de Aceite

Para certificar que o processamento batch foi concluído com sucesso:
1. O DataFrame `df` deve exibir todas as linhas do dataset JSON sem colunas corrompidas (`_corrupt_record`).
2. A coluna `valor_total_estoque` deve estar devidamente arredondada para 2 casas decimais.
3. A agregação analítica `df_resumo` deve listar produtos únicos com suas métricas consolidadas (`preco_medio` e `total_quantidade`).

---

## 🧹 Cleanup (Limpeza do Ambiente)

Ao finalizar a prática e exportar seu notebook, efetue a limpeza dos recursos:
1. **Apagar o Volume:** Vá no menu **Catalog** -> `workspace` -> `default` -> deletar o Volume **`teste`**.
2. **Apagar o Notebook:** Vá em **Workspace** -> `Users` -> deletar o notebook de testes.

---

## 💡 Desafios Complementares

* **Schema Estrito (Production-Ready):** Substitua a inferência automática (`inferSchema: true`) por uma declaração explícita de `StructType` e `StructField`, prevenindo custos de inferência em arquivos massivos.
* **Escrita em Formato Parquet:** Adicione uma célula final persistindo `df_transformado` em formato colunar Parquet particionado por categoria de produto:
  ```python
  df_transformado.write.mode("overwrite").partitionBy("nome").parquet("/Volumes/workspace/default/teste/output_parquet/")
  ```
