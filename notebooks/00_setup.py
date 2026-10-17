# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup: schema, volume e arquivos de origem
# MAGIC
# MAGIC **O que faz:**
# MAGIC 1. Cria o **schema** `workspace.industrial` (onde ficam as tabelas Delta).
# MAGIC 2. Cria o **volume** `workspace.industrial.arquivos` (onde ficam os arquivos CSV).
# MAGIC 3. Coloca o CSV do **AI4I 2020** no volume: tenta baixar do UCI; se a internet estiver
# MAGIC    bloqueada (comum no Free Edition), usa a cópia que vem neste repositório (`dados/`).
# MAGIC 4. Gera as **leituras simuladas** (semente 42, mesmo simulador do repositório base).
# MAGIC
# MAGIC **Pode rodar quantas vezes quiser:** tudo usa `IF NOT EXISTS` ou sobrescreve o mesmo arquivo.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md ## 1. Schema e volume (Unity Catalog)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_COMPLETO} COMMENT 'Pipeline industrial: bronze, silver e gold'")

if EM_DATABRICKS:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA_COMPLETO}.{VOLUME} COMMENT 'Arquivos de origem (CSV)'")
else:
    os.makedirs(PASTA_VOLUME, exist_ok=True)  # teste local: o "volume" é só uma pasta

print(f"schema {SCHEMA_COMPLETO} e volume {PASTA_VOLUME} prontos")

# COMMAND ----------

# MAGIC %md ## 2. AI4I 2020 (CSV público do UCI)

# COMMAND ----------

import io
import shutil
import urllib.request
import zipfile

os.makedirs(os.path.dirname(ARQUIVO_AI4I), exist_ok=True)
# Cópia que vem no repositório (Git folder): ../dados/ai4i2020.csv a partir desta pasta
COPIA_NO_REPO = os.path.abspath(os.path.join(os.getcwd(), "..", "dados", "ai4i2020.csv"))


def baixar_do_uci() -> bytes:
    req = urllib.request.Request(URL_AI4I, headers={"User-Agent": "pipeline-industrial-databricks"})
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 (URL fixa, https)
        with zipfile.ZipFile(io.BytesIO(resp.read())) as zf:
            return zf.read("ai4i2020.csv")


if os.path.exists(ARQUIVO_AI4I):
    origem = "já estava no volume"
else:
    try:
        conteudo = baixar_do_uci()
        with open(ARQUIVO_AI4I, "wb") as f:
            f.write(conteudo)
        origem = "baixado do UCI"
    except Exception as erro:  # Free Edition: internet de saída restrita
        print(f"download do UCI falhou ({type(erro).__name__}: {erro}); usando a cópia do repositório")
        if not os.path.exists(COPIA_NO_REPO):
            raise FileNotFoundError(
                "Não achei o CSV do AI4I. Faça o upload manual de ai4i2020.csv para "
                f"{os.path.dirname(ARQUIVO_AI4I)} (Catalog > {CATALOGO} > {SCHEMA} > {VOLUME} > "
                "Upload to this volume) e rode esta célula de novo."
            ) from erro
        shutil.copyfile(COPIA_NO_REPO, ARQUIVO_AI4I)
        origem = f"copiado de {COPIA_NO_REPO}"

with open(ARQUIVO_AI4I, encoding="utf-8") as f:
    n_linhas = sum(1 for _ in f) - 1  # menos o cabeçalho
print(f"AI4I: {ARQUIVO_AI4I} ({origem}) — {n_linhas} linhas")
assert n_linhas == 10_000, "o AI4I 2020 tem exatamente 10.000 linhas"

# COMMAND ----------

# MAGIC %md ## 3. Leituras simuladas (semente 42)
# MAGIC
# MAGIC O simulador (`simulador.py`, nesta mesma pasta) gera 8 máquinas × 7 dias, com problemas
# MAGIC **injetados de propósito** (nulos, outliers, texto "ERRO", duplicadas) para a camada
# MAGIC silver ter o que limpar.

# COMMAND ----------

import sys

sys.path.insert(0, os.getcwd())  # garante que "simulador.py" desta pasta seja encontrado
from simulador import gerar_leituras  # noqa: E402

os.makedirs(os.path.dirname(ARQUIVO_SIMULADO), exist_ok=True)
df_sim, resumo = gerar_leituras(semente=SEMENTE_SIMULADOR)
df_sim.to_csv(ARQUIVO_SIMULADO, index=False)
print(f"simulado: {ARQUIVO_SIMULADO} — {len(df_sim)} linhas; injetado: {resumo}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pronto
# MAGIC Arquivos no volume. Próximo passo: **01_bronze**.
