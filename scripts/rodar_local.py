"""Roda os notebooks FORA do Databricks, com PySpark + Delta Lake locais (teste/CI).

Não é necessário para usar no Databricks. Serve para conferir a lógica no seu PC ou no CI:

    pip install -r requirements-local.txt      # precisa de Java 17+
    python scripts/rodar_local.py

No fim, roda as consultas de dashboards/consultas.sql (cada uma precisa devolver linhas).

O que ele faz: lê cada notebook .py, separa as células (# COMMAND ----------), ignora as células
de Markdown (# MAGIC %md), troca "%run ./_config" pelo conteúdo do _config e executa tudo com uma
SparkSession local. O catálogo fica vazio (tabelas "industrial.<nome>") e o volume vira uma pasta temporária.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
PASTA_NOTEBOOKS = RAIZ / "notebooks"
NOTEBOOKS = ["00_setup", "01_bronze", "02_silver", "03_gold", "04_qualidade"]


def celulas(caminho: Path) -> list[str]:
    texto = caminho.read_text(encoding="utf-8")
    saida = []
    for bloco in texto.split("# COMMAND ----------"):
        linhas = [ln for ln in bloco.splitlines() if ln.strip() != "# Databricks notebook source"]
        magicas = [ln for ln in linhas if ln.startswith("# MAGIC")]
        if any(ln.strip() == "# MAGIC %run ./_config" for ln in magicas):
            saida.extend(celulas(PASTA_NOTEBOOKS / "_config.py"))
        elif magicas:
            continue  # célula de Markdown
        else:
            saida.append("\n".join(linhas))
    return saida


def rodar_consultas(spark) -> None:
    """Executa cada consulta de dashboards/consultas.sql (troca workspace.industrial. por industrial.)."""
    arquivo = RAIZ / "dashboards" / "consultas.sql"
    if not arquivo.exists():
        return
    texto = "\n".join(ln for ln in arquivo.read_text(encoding="utf-8").splitlines() if not ln.strip().startswith("--"))
    schema = os.environ.get("PI_SCHEMA", "industrial")
    for i, sql in enumerate((c.strip() for c in texto.split(";")), start=1):
        if not sql:
            continue
        print(f"\n===== consulta [{i}] =====", flush=True)
        df = spark.sql(sql.replace("workspace.industrial.", f"{schema}."))
        assert df.count() > 0, f"consulta [{i}] não devolveu linhas"
        df.show(30, truncate=False)


def main() -> None:
    base = Path(os.environ.get("PI_TMP", tempfile.mkdtemp(prefix="pi-databricks-")))
    os.environ.setdefault("PI_CATALOGO", "")  # sem Unity Catalog: tabelas em "industrial.<nome>"
    os.environ.setdefault("PI_PASTA_VOLUME", str(base / "volume"))

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    construtor = (
        SparkSession.builder.master("local[2]")
        .appName("pipeline-industrial-local")
        .config("spark.driver.memory", os.environ.get("PI_DRIVER_MEMORY", "1g"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")  # como no serverless do Databricks
        .config("spark.sql.warehouse.dir", str(base / "warehouse"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(construtor).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    def display(df, n: int = 30):
        df.show(n, truncate=False)

    os.chdir(PASTA_NOTEBOOKS)
    sys.path.insert(0, str(PASTA_NOTEBOOKS))
    selecionados = sys.argv[1:] or NOTEBOOKS
    try:
        for nome in selecionados:
            print(f"\n===== {nome} =====", flush=True)
            escopo = {"spark": spark, "display": display, "__name__": "__notebook__"}
            for codigo in celulas(PASTA_NOTEBOOKS / f"{nome}.py"):
                exec(compile(codigo, f"{nome}.py", "exec"), escopo)  # noqa: S102 (código do próprio repo)
        rodar_consultas(spark)
    finally:
        spark.stop()
    print("\nOK: todos os notebooks rodaram localmente.")


if __name__ == "__main__":
    main()
