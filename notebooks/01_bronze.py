# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: ingestão bruta em Delta
# MAGIC
# MAGIC **O que é a camada bronze:** os dados **como chegaram**, sem limpar nada (tudo como texto).
# MAGIC Se a limpeza tiver um erro, dá para refazer a partir daqui sem buscar a fonte de novo.
# MAGIC
# MAGIC **O que este notebook faz:**
# MAGIC 1. Lê os dois CSVs do volume (AI4I e simulado).
# MAGIC 2. Converte o AI4I para o mesmo formato do simulado (máquinas virtuais + horário), como no
# MAGIC    repositório base.
# MAGIC 3. Acrescenta **metadados de carga**: `_id_carga`, `_carregado_em`, `_arquivo_origem`, `_linha`.
# MAGIC 4. Grava a tabela Delta `bronze_leituras` (sobrescreve: rodar de novo não duplica).
# MAGIC 5. Registra a execução na tabela `cargas` (histórico, usado na checagem de freshness).

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import uuid

from pyspark.sql import functions as F

ID_CARGA = str(uuid.uuid4())
CARREGADO_EM = F.current_timestamp()


def ler_csv_texto(caminho: str):
    """Lê um CSV com TODAS as colunas como texto (inferSchema desligado = nada é convertido)."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(caminho)
        # posição da linha no arquivo: usada para decidir qual duplicada fica (a primeira)
        .withColumn("_linha", F.monotonically_increasing_id())
    )


# COMMAND ----------

# MAGIC %md ## AI4I → formato bruto comum

# COMMAND ----------

cru = ler_csv_texto(ARQUIVO_AI4I)
udi = F.col("UDI").cast("int")
maquina = (udi - 1) % AI4I_MAQUINAS_VIRTUAIS + 1

# Modos de falha: junta os nomes das colunas TWF/HDF/... que valem "1", separados por "|"
modos = F.concat_ws("|", *[F.when(F.col(m) == "1", F.lit(m)) for m in MODOS_FALHA])

ai4i = cru.select(
    F.lit("ai4i").alias("fonte"),
    F.concat(F.lit("AI4I-"), F.lpad(maquina.cast("string"), 2, "0")).alias("maquina_id"),
    # TIMESTAMP_NTZ = horário "sem fuso", assim o resultado não depende do fuso da sessão
    F.date_format(
        F.expr(
            f"timestampadd(MINUTE, floor((cast(UDI as int) - 1) / {AI4I_MAQUINAS_VIRTUAIS}) * {AI4I_INTERVALO_MIN}, "
            f"to_timestamp_ntz('{AI4I_INICIO}'))"
        ),
        "yyyy-MM-dd HH:mm:ss",
    ).alias("timestamp"),
    F.col("Type").alias("tipo"),
    F.col("Air temperature [K]").alias("temp_ar_k"),
    F.col("Process temperature [K]").alias("temp_processo_k"),
    F.col("Rotational speed [rpm]").alias("rotacao_rpm"),
    F.col("Torque [Nm]").alias("torque_nm"),
    F.col("Tool wear [min]").alias("desgaste_min"),
    F.col("Machine failure").alias("falha"),
    F.when(modos != "", modos).alias("modo_falha"),
    F.lit(ARQUIVO_AI4I).alias("_arquivo_origem"),
    "_linha",
)

# COMMAND ----------

# MAGIC %md ## Simulado (já vem no formato bruto)

# COMMAND ----------

simulado = ler_csv_texto(ARQUIVO_SIMULADO).select(*COLUNAS_BRUTO, F.lit(ARQUIVO_SIMULADO).alias("_arquivo_origem"), "_linha")

# COMMAND ----------

# MAGIC %md ## Gravar `bronze_leituras` + metadados de carga

# COMMAND ----------

bronze = ai4i.unionByName(simulado).select(
    *COLUNAS_BRUTO,
    F.lit(ID_CARGA).alias("_id_carga"),
    CARREGADO_EM.alias("_carregado_em"),
    "_arquivo_origem",
    "_linha",
)

(bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela("bronze_leituras")))

por_fonte = {
    r["fonte"]: r["n"] for r in spark.table(tabela("bronze_leituras")).groupBy("fonte").agg(F.count("*").alias("n")).collect()
}
total = sum(por_fonte.values())
print(f"bronze_leituras: {total} linhas {por_fonte} (carga {ID_CARGA})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Histórico de cargas
# MAGIC Uma linha por execução (append). O notebook 04 usa `carregado_em` para saber se os dados
# MAGIC estão atualizados (*freshness*).

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {tabela("cargas")} (
        id_carga STRING, carregado_em TIMESTAMP, linhas_bronze BIGINT,
        linhas_ai4i BIGINT, linhas_simulado BIGINT
    ) USING DELTA
    """
)
(
    spark.createDataFrame(
        [(ID_CARGA, total, por_fonte.get("ai4i", 0), por_fonte.get("simulado", 0))],
        "id_carga STRING, linhas_bronze BIGINT, linhas_ai4i BIGINT, linhas_simulado BIGINT",
    )
    .withColumn("carregado_em", F.current_timestamp())
    .select("id_carga", "carregado_em", "linhas_bronze", "linhas_ai4i", "linhas_simulado")
    .write.format("delta")
    .mode("append")
    .saveAsTable(tabela("cargas"))
)
display(spark.table(tabela("cargas")).orderBy(F.desc("carregado_em")).limit(5))
