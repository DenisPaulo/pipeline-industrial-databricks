# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: modelo estrela (star schema)
# MAGIC
# MAGIC **O que é a camada gold:** tabelas **finais**, pensadas para análise e painéis.
# MAGIC São as mesmas do dbt no repositório base, agora em PySpark + Delta:
# MAGIC
# MAGIC | Tabela | Tipo | Uma linha por |
# MAGIC |---|---|---|
# MAGIC | `gold_dim_maquina` | dimensão ("quem") | máquina |
# MAGIC | `gold_fct_leituras` | fato ("o que aconteceu") | leitura |
# MAGIC | `gold_fct_falhas_diarias` | fato agregado | máquina + dia com falha |
# MAGIC
# MAGIC Os fatos apontam para a dimensão pela coluna `id_maquina`.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

leituras = spark.table(tabela("silver_leituras"))
falhas = spark.table(tabela("silver_falhas"))
ultima_carga = spark.table(tabela("cargas")).agg(F.max("carregado_em")).first()[0]


def salvar(df, nome: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela(nome))
    print(f"{nome}: {spark.table(tabela(nome)).count()} linhas")


def celsius(col_kelvin: str):
    # Kelvin -> Celsius com 2 casas (decimal evita "lixo" de ponto flutuante tipo 24.999999)
    return F.round((F.col(col_kelvin) - F.lit(273.15)).cast("decimal(10,4)"), 2)


# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_fct_leituras
# MAGIC Cada leitura ganha um `id_leitura` estável (ordem máquina + horário) e os modos de falha
# MAGIC agregados em texto, em ordem alfabética (ex.: `HDF, PWF`).

# COMMAND ----------

modos_por_leitura = falhas.groupBy("maquina_id", "ts").agg(
    F.concat_ws(", ", F.array_sort(F.collect_list("modo"))).alias("modos_falha"),
    F.count("*").alias("qtd_modos_falha"),
)

fct_leituras = leituras.join(modos_por_leitura, ["maquina_id", "ts"], "left").select(
    F.row_number().over(Window.orderBy("maquina_id", "ts")).cast("bigint").alias("id_leitura"),
    F.col("maquina_id").alias("id_maquina"),
    F.col("ts").alias("medido_em"),
    "tipo_produto",
    celsius("temp_ar_k").alias("temperatura_ar_c"),
    celsius("temp_processo_k").alias("temperatura_processo_c"),
    "rotacao_rpm",
    "torque_nm",
    "desgaste_min",
    F.col("falha").alias("houve_falha"),
    "modos_falha",
    F.coalesce("qtd_modos_falha", F.lit(0)).cast("int").alias("qtd_modos_falha"),
)
salvar(fct_leituras, "gold_fct_leituras")

# COMMAND ----------

# MAGIC %md ## gold_dim_maquina

# COMMAND ----------

dim_maquina = (
    leituras.groupBy(F.col("maquina_id").alias("id_maquina"))
    .agg(
        F.first("fonte").alias("fonte"),
        F.count("*").alias("total_leituras"),
        F.count(F.when(F.col("falha"), 1)).alias("total_falhas"),
    )
    .withColumn("carregada_em", F.lit(ultima_carga).cast("timestamp"))
    .select("id_maquina", "fonte", "carregada_em", "total_leituras", "total_falhas")
)
salvar(dim_maquina, "gold_dim_maquina")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_fct_falhas_diarias
# MAGIC Quantas leituras com falha cada máquina teve por dia, e quais conjuntos de modos apareceram.

# COMMAND ----------

fct_falhas_diarias = (
    spark.table(tabela("gold_fct_leituras"))
    .where("houve_falha")
    .groupBy("id_maquina", F.to_date("medido_em").alias("data_ref"))
    .agg(
        F.count("*").alias("qtd_falhas"),
        F.concat_ws("; ", F.array_sort(F.collect_set("modos_falha"))).alias("modos_falha"),
    )
)
salvar(fct_falhas_diarias, "gold_fct_falhas_diarias")

# COMMAND ----------

# MAGIC %md ## Exemplo de consulta: máquinas com mais falhas

# COMMAND ----------

display(spark.table(tabela("gold_dim_maquina")).orderBy(F.desc("total_falhas")).limit(10))
