# Databricks notebook source
# MAGIC %md
# MAGIC # Configuração compartilhada
# MAGIC
# MAGIC **O que é:** constantes usadas por todos os notebooks (nomes de catálogo, schema, volume e
# MAGIC tabelas; regras de limpeza). Os outros notebooks carregam este arquivo com `%run ./_config`.
# MAGIC
# MAGIC **Pra que serve:** mudar um nome (por exemplo, o schema) num lugar só.
# MAGIC
# MAGIC No **Databricks Free Edition** o catálogo padrão se chama `workspace`.

# COMMAND ----------

import os

# Unity Catalog: catálogo.schema.tabela (três níveis).
# As variáveis de ambiente PI_* só existem no teste local (scripts/rodar_local.py).
CATALOGO = os.environ.get("PI_CATALOGO", "workspace")
SCHEMA = os.environ.get("PI_SCHEMA", "industrial")
VOLUME = "arquivos"  # Unity Catalog Volume: "pasta" governada para guardar arquivos (CSV, ZIP...)

# Caminho do volume. No Databricks é sempre /Volumes/<catálogo>/<schema>/<volume>.
PASTA_VOLUME = os.environ.get("PI_PASTA_VOLUME", f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}")
ARQUIVO_AI4I = f"{PASTA_VOLUME}/ai4i/ai4i2020.csv"
ARQUIVO_SIMULADO = f"{PASTA_VOLUME}/simulado/leituras_simuladas.csv"

# Estamos rodando dentro do Databricks? (no teste local, não)
EM_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ


# No teste local não existe Unity Catalog: o catálogo fica vazio e o nome vira schema.tabela.
SCHEMA_COMPLETO = f"{CATALOGO}.{SCHEMA}" if CATALOGO else SCHEMA


def tabela(nome: str) -> str:
    """Nome completo de uma tabela: catalogo.schema.nome."""
    return f"{SCHEMA_COMPLETO}.{nome}"


# --- Fontes ----------------------------------------------------------------------------------
URL_AI4I = "https://archive.ics.uci.edu/static/public/601/ai4i+2020+predictive+maintenance+dataset.zip"
SEMENTE_SIMULADOR = 42  # mesma semente do repositório base => mesmos dados simulados

# O AI4I não tem máquina nem horário: como no repositório base, cada linha (UDI) vira uma de
# 10 "máquinas virtuais" AI4I-01..AI4I-10, com uma leitura a cada 10 minutos desde 2020-01-01.
AI4I_MAQUINAS_VIRTUAIS = 10
AI4I_INICIO = "2020-01-01 00:00:00"
AI4I_INTERVALO_MIN = 10

# --- Contrato das colunas (camada bronze: tudo texto, como chegou) ---------------------------
SENSORES = ["temp_ar_k", "temp_processo_k", "rotacao_rpm", "torque_nm", "desgaste_min"]
COLUNAS_BRUTO = ["fonte", "maquina_id", "timestamp", "tipo", *SENSORES, "falha", "modo_falha"]

# --- Regras da limpeza (iguais às do repositório base) -------------------------------------
MODOS_FALHA = ["TWF", "HDF", "PWF", "OSF", "RNF"]
MODO_DESCONHECIDO = "DESCONHECIDO"
TIPOS_PRODUTO = ["L", "M", "H"]
FAIXAS_FISICAS = {  # faixas fisicamente plausíveis (largas: pegam erro de sensor/unidade)
    "temp_ar_k": (250.0, 350.0),
    "temp_processo_k": (250.0, 400.0),
    "rotacao_rpm": (0.0, 10_000.0),
    "torque_nm": (0.0, 200.0),
    "desgaste_min": (0.0, 1_000.0),
}
# Cada linha descartada recebe UM motivo: o primeiro desta lista que se aplica.
MOTIVOS = [
    "duplicada",
    "maquina_ausente",
    "timestamp_invalido",
    "tipo_invalido",
    "falha_invalida",
    "sensor_nulo",
    "sensor_nao_numerico",
    "fora_da_faixa_fisica",
]

# --- Limites da qualidade (notebook 04) -----------------------------------------------------
VOLUME_MINIMO_BRONZE = 26_000
VOLUME_MINIMO_SILVER = 24_000
FRESHNESS_MAX_HORAS = 48

print(f"[config] tabelas em {SCHEMA_COMPLETO} | arquivos em {PASTA_VOLUME}")
