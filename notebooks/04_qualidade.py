# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Qualidade: checagens que falham com `assert`
# MAGIC
# MAGIC **Pra que serve:** garantir que as tabelas estão certas **antes** de alguém usar.
# MAGIC Se alguma checagem falhar, o notebook para com erro (num Job, a execução fica vermelha).
# MAGIC
# MAGIC | Tipo | Pergunta |
# MAGIC |---|---|
# MAGIC | **Volume** | Chegou a quantidade esperada de linhas? |
# MAGIC | **Conservação** | Nenhuma linha sumiu entre as camadas? |
# MAGIC | **Nulos** | As colunas obrigatórias estão preenchidas? |
# MAGIC | **Unicidade** | As chaves não se repetem? |
# MAGIC | **Relacionamento** | Todo fato aponta para uma máquina que existe? |
# MAGIC | **Valores aceitos** | `tipo_produto` só L/M/H, `fonte` só ai4i/simulado? |
# MAGIC | **Freshness** | A última carga foi há menos de 48 h? |

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F

t = {n: spark.table(tabela(n)) for n in [
    "bronze_leituras", "silver_leituras", "silver_descartes", "silver_falhas",
    "gold_dim_maquina", "gold_fct_leituras", "gold_fct_falhas_diarias", "cargas",
]}  # fmt: skip
n = {k: v.count() for k, v in t.items()}
resultados = []


def checar(nome: str, ok: bool, detalhe: str) -> None:
    resultados.append((nome, "PASS" if ok else "FAIL", detalhe))


def duplicadas(df, chaves) -> int:
    return df.groupBy(*chaves).count().where("count > 1").count()


def nulos(df, colunas) -> int:
    return df.where(" OR ".join(f"{c} IS NULL" for c in colunas)).count()


# COMMAND ----------

# Volume
checar("volume_bronze", n["bronze_leituras"] >= VOLUME_MINIMO_BRONZE, f"{n['bronze_leituras']} >= {VOLUME_MINIMO_BRONZE}")
checar("volume_silver", n["silver_leituras"] >= VOLUME_MINIMO_SILVER, f"{n['silver_leituras']} >= {VOLUME_MINIMO_SILVER}")
checar("volume_fct_falhas_diarias", n["gold_fct_falhas_diarias"] >= 1, f"{n['gold_fct_falhas_diarias']} >= 1")

# Conservação entre camadas
soma = n["silver_leituras"] + n["silver_descartes"]
checar("bronze_igual_silver_mais_descartes", soma == n["bronze_leituras"], f"{soma} == {n['bronze_leituras']}")
checar(
    "fct_leituras_sem_perda",
    n["gold_fct_leituras"] == n["silver_leituras"],
    f"{n['gold_fct_leituras']} == {n['silver_leituras']}",
)
soma_dim = t["gold_dim_maquina"].agg(F.sum("total_leituras")).first()[0]
checar("dim_maquina_soma_leituras", soma_dim == n["gold_fct_leituras"], f"{soma_dim} == {n['gold_fct_leituras']}")
com_falha = t["gold_fct_leituras"].where("houve_falha").count()
soma_falhas = t["gold_fct_falhas_diarias"].agg(F.sum("qtd_falhas")).first()[0]
checar("falhas_diarias_somam_leituras_com_falha", soma_falhas == com_falha, f"{soma_falhas} == {com_falha}")

# Nulos
checar(
    "sem_nulos_fct_leituras",
    nulos(t["gold_fct_leituras"], ["id_leitura", "id_maquina", "medido_em", "tipo_produto", "houve_falha"]) == 0,
    "chaves e colunas obrigatórias",
)
checar("sem_nulos_silver_sensores", nulos(t["silver_leituras"], SENSORES) == 0, ", ".join(SENSORES))

# Unicidade
checar("unico_id_leitura", duplicadas(t["gold_fct_leituras"], ["id_leitura"]) == 0, "gold_fct_leituras.id_leitura")
checar("unico_maquina_horario", duplicadas(t["silver_leituras"], ["maquina_id", "ts"]) == 0, "silver_leituras (maquina_id, ts)")
checar("unico_id_maquina", duplicadas(t["gold_dim_maquina"], ["id_maquina"]) == 0, "gold_dim_maquina.id_maquina")
checar(
    "unico_maquina_dia",
    duplicadas(t["gold_fct_falhas_diarias"], ["id_maquina", "data_ref"]) == 0,
    "gold_fct_falhas_diarias (id_maquina, data_ref)",
)

# Relacionamento (fato -> dimensão)
orfas = t["gold_fct_leituras"].join(t["gold_dim_maquina"], "id_maquina", "left_anti").count()
checar("fct_leituras_aponta_para_dim", orfas == 0, f"{orfas} leituras sem máquina na dimensão")

# Valores aceitos
tipos_ruins = t["gold_fct_leituras"].where(~F.col("tipo_produto").isin(TIPOS_PRODUTO)).count()
checar("tipo_produto_valido", tipos_ruins == 0, "L, M ou H")
fontes_ruins = t["gold_dim_maquina"].where(~F.col("fonte").isin("ai4i", "simulado")).count()
checar("fonte_valida", fontes_ruins == 0, "ai4i ou simulado")

# Freshness (a última carga é recente?)
horas = (
    t["cargas"]
    .agg(((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max("carregado_em"))) / 3600).alias("h"))
    .first()["h"]
)
checar(
    "freshness_cargas",
    horas is not None and horas < FRESHNESS_MAX_HORAS,
    f"última carga há {horas or 0:.2f} h (máx. {FRESHNESS_MAX_HORAS} h)",
)

# COMMAND ----------

relatorio = spark.createDataFrame(resultados, "checagem STRING, resultado STRING, detalhe STRING")
display(relatorio)

falhou = [r[0] for r in resultados if r[1] == "FAIL"]
print(f"{len(resultados) - len(falhou)} de {len(resultados)} checagens passaram")
assert not falhou, f"checagens com falha: {falhou}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparação com o repositório base (informativo, não falha)
# MAGIC Com a mesma entrada (AI4I + simulado semente 42), o pipeline Pandas + PostgreSQL + dbt do
# MAGIC repositório base tem: **24.511** leituras, **586** registros de falha, **18** máquinas e
# MAGIC **123** linhas em `fct_falhas_diarias` (soma de `qtd_falhas` = **556**).

# COMMAND ----------

referencia = {
    "silver_leituras": 24_511,
    "silver_falhas": 586,
    "gold_dim_maquina": 18,
    "gold_fct_falhas_diarias": 123,
    "soma_qtd_falhas": 556,
}
atual = {**{k: n[k] for k in referencia if k in n}, "soma_qtd_falhas": soma_falhas}
comparacao = [(k, referencia[k], int(atual[k]), "igual" if referencia[k] == atual[k] else "DIFERENTE") for k in referencia]
display(spark.createDataFrame(comparacao, "medida STRING, repo_base BIGINT, databricks BIGINT, situacao STRING"))

# No CI (variável PI_EXIGIR_PARIDADE=1) a diferença com o repositório base também falha.
if os.environ.get("PI_EXIGIR_PARIDADE") == "1":
    assert all(c[3] == "igual" for c in comparacao), f"diferente do repositório base: {comparacao}"
