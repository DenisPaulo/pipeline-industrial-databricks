# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: limpeza, tipos, duplicadas e regras físicas
# MAGIC
# MAGIC **O que é a camada silver:** dados **limpos e tipados**, prontos para virar tabelas de análise.
# MAGIC
# MAGIC **Regras (as mesmas da limpeza em Pandas do repositório base):** cada linha ruim recebe
# MAGIC **um único motivo**, o primeiro que se aplica nesta ordem:
# MAGIC
# MAGIC | Motivo | Quando |
# MAGIC |---|---|
# MAGIC | `duplicada` | mesma fonte + máquina + horário de uma linha anterior |
# MAGIC | `maquina_ausente` | sem `maquina_id` |
# MAGIC | `timestamp_invalido` | horário vazio ou fora do formato `AAAA-MM-DD HH:MM:SS` |
# MAGIC | `tipo_invalido` | tipo de produto diferente de L, M, H |
# MAGIC | `falha_invalida` | `falha` diferente de 0 ou 1 |
# MAGIC | `sensor_nulo` | algum sensor vazio |
# MAGIC | `sensor_nao_numerico` | algum sensor com texto (ex.: `ERRO`) |
# MAGIC | `fora_da_faixa_fisica` | valor impossível (ex.: 999,9 K, rotação negativa) |
# MAGIC
# MAGIC Saídas: `silver_leituras`, `silver_falhas` (um registro por modo de falha) e
# MAGIC `silver_descartes` (as linhas rejeitadas, com o motivo).

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

bronze = spark.table(tabela("bronze_leituras"))
n_bronze = bronze.count()

# 1) tira espaços em volta de todos os textos ("  12.0 " -> "12.0")
df = bronze.select(*[F.trim(c).alias(c) for c in COLUNAS_BRUTO], "_id_carga", "_linha")


def vazio(c: str):
    return F.col(c).isNull() | (F.col(c) == "")


# 2) conversões "tentativas": se não der para converter, vira NULL (em vez de quebrar)
ts = F.try_to_timestamp(F.col("timestamp"), F.lit("yyyy-MM-dd HH:mm:ss"))
num = {c: F.expr(f"try_cast({c} AS DOUBLE)") for c in SENSORES}


def qualquer(condicoes):
    """Verdadeiro se pelo menos uma condição for verdadeira (NULL conta como falso)."""
    out = F.lit(False)
    for cond in condicoes:
        out = out | F.coalesce(cond, F.lit(False))
    return out


# 3) cada regra vira uma coluna verdadeiro/falso
ordem_na_chave = F.row_number().over(Window.partitionBy("fonte", "maquina_id", "timestamp").orderBy("_linha"))
regras = {
    "duplicada": (ordem_na_chave > 1) & ~vazio("maquina_id") & ts.isNotNull(),
    "maquina_ausente": vazio("maquina_id"),
    "timestamp_invalido": ts.isNull(),
    "tipo_invalido": ~F.coalesce(F.col("tipo").isin(TIPOS_PRODUTO), F.lit(False)),
    "falha_invalida": ~F.coalesce(F.col("falha").isin("0", "1"), F.lit(False)),
    "sensor_nulo": qualquer([vazio(c) for c in SENSORES]),
    "sensor_nao_numerico": qualquer([num[c].isNull() & ~vazio(c) for c in SENSORES]),
    "fora_da_faixa_fisica": qualquer([(num[c] < FAIXAS_FISICAS[c][0]) | (num[c] > FAIXAS_FISICAS[c][1]) for c in SENSORES]),
}

# 4) motivo = a PRIMEIRA regra verdadeira, na ordem de MOTIVOS (coalesce pega o 1º não nulo)
motivo = F.coalesce(*[F.when(regras[m], F.lit(m)) for m in MOTIVOS])
classificado = df.withColumn("motivo", motivo).withColumn("_ts", ts)
for c in SENSORES:
    classificado = classificado.withColumn(f"_num_{c}", num[c])

# COMMAND ----------

# MAGIC %md ## Descartes (com contagem por motivo)

# COMMAND ----------

descartes = classificado.where(F.col("motivo").isNotNull()).select(*COLUNAS_BRUTO, "motivo", "_id_carga", "_linha")
descartes.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela("silver_descartes"))

por_motivo = spark.table(tabela("silver_descartes")).groupBy("motivo").count().orderBy(F.desc("count"))
display(por_motivo)

# COMMAND ----------

# MAGIC %md ## Leituras válidas (tipadas)

# COMMAND ----------

validas = classificado.where(F.col("motivo").isNull())
leituras = validas.select(
    "fonte",
    "maquina_id",
    F.col("_ts").alias("ts"),
    F.col("tipo").alias("tipo_produto"),
    F.col("_num_temp_ar_k").alias("temp_ar_k"),
    F.col("_num_temp_processo_k").alias("temp_processo_k"),
    # bround = arredondamento "bancário", igual ao Pandas do repositório base
    F.bround("_num_rotacao_rpm").cast("int").alias("rotacao_rpm"),
    F.col("_num_torque_nm").alias("torque_nm"),
    F.bround("_num_desgaste_min").cast("int").alias("desgaste_min"),
    (F.col("falha") == "1").alias("falha"),
    "_id_carga",
)
leituras.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela("silver_leituras"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Falhas: um registro por modo
# MAGIC `"HDF|PWF"` vira duas linhas. Falha sem modo informado vira `DESCONHECIDO`.

# COMMAND ----------

lista = F.filter(F.coalesce(F.split("modo_falha", r"\|"), F.array()), lambda m: m.isin(MODOS_FALHA))
falhas = (
    validas.where(F.col("falha") == "1")
    .select(
        "maquina_id",
        F.col("_ts").alias("ts"),
        F.explode(F.when(F.size(lista) == 0, F.array(F.lit(MODO_DESCONHECIDO))).otherwise(lista)).alias("modo"),
    )
    .distinct()
)
falhas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela("silver_falhas"))

# COMMAND ----------

# MAGIC %md ## Conferência: entrada = válidas + descartadas

# COMMAND ----------

n_validas = spark.table(tabela("silver_leituras")).count()
n_desc = spark.table(tabela("silver_descartes")).count()
n_falhas = spark.table(tabela("silver_falhas")).count()
print(f"bronze {n_bronze} = válidas {n_validas} + descartadas {n_desc}; registros de falha: {n_falhas}")
assert n_validas + n_desc == n_bronze, "a conta não fecha: alguma linha sumiu ou duplicou"
