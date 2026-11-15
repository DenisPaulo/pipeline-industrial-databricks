"""Spark local com Delta e a amostra que atravessa bronze, silver e gold."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
from amostra import gravar
from rodar_local import criar_spark, executar_notebooks


@pytest.fixture(scope="session")
def spark():
    # O metastore Derby usa o diretório do processo. A sessão de teste fica numa pasta temporária.
    base = Path(tempfile.mkdtemp(prefix="pi-pytest-"))
    os.chdir(base)
    sessao = criar_spark(base)
    yield sessao
    sessao.stop()


@pytest.fixture(scope="session")
def pipeline(spark):
    volume = Path(tempfile.mkdtemp(prefix="pi-volume-"))
    gravar(volume)
    os.environ["PI_CATALOGO"] = ""
    os.environ["PI_SCHEMA"] = "industrial"
    os.environ["PI_PASTA_VOLUME"] = str(volume)
    spark.sql("CREATE SCHEMA IF NOT EXISTS industrial")
    executar_notebooks(
        spark,
        ["01_bronze", "02_silver", "03_gold"],
        mudar_diretorio=False,
        mostrar=False,
    )
    return spark
