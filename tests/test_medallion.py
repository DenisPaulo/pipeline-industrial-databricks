"""Transformações bronze → silver → gold com SparkSession local e CSV sintético."""

from __future__ import annotations

import pytest
from amostra import N_AI4I, SIMULADO, motivos_esperados
from pyspark.sql import functions as F


def test_bronze_mantem_sensores_como_texto_e_grava_delta(pipeline):
    bronze = pipeline.table("industrial.bronze_leituras")
    tipos = dict(bronze.dtypes)
    for coluna in ("temp_ar_k", "temp_processo_k", "rotacao_rpm", "torque_nm", "desgaste_min", "falha"):
        assert tipos[coluna] == "string", coluna
    for coluna in ("_id_carga", "_carregado_em", "_arquivo_origem", "_linha"):
        assert bronze.where(F.col(coluna).isNull()).count() == 0, coluna
    cargas = {r["_id_carga"] for r in bronze.select("_id_carga").distinct().collect()}
    assert len(cargas) == 1
    origens = {r["_arquivo_origem"] for r in bronze.select("_arquivo_origem").distinct().collect()}
    assert any(origem.endswith("ai4i2020.csv") for origem in origens)
    assert any(origem.endswith("leituras_simuladas.csv") for origem in origens)
    formato = pipeline.sql("DESCRIBE DETAIL industrial.bronze_leituras").select("format").first().format
    assert formato == "delta"
    assert bronze.where("fonte = 'ai4i'").count() == N_AI4I
    assert bronze.where("fonte = 'simulado'").count() == len(SIMULADO)


def test_bronze_padroniza_maquinas_e_horario_do_ai4i(pipeline):
    linhas = {
        (r.maquina_id, r.timestamp, r.tipo, r.falha, r.modo_falha, r.temp_ar_k, r.rotacao_rpm)
        for r in pipeline.table("industrial.bronze_leituras").where("fonte = 'ai4i'").collect()
    }
    assert linhas == {
        ("AI4I-01", "2020-01-01 00:00:00", "M", "0", None, "298.1", "1551"),
        ("AI4I-02", "2020-01-01 00:00:00", "L", "0", None, "298.2", "1408"),
        ("AI4I-01", "2020-01-01 00:10:00", "L", "1", "HDF", "298.3", "1450"),
    }


def test_silver_classifica_um_motivo_por_regra(pipeline):
    obtido = {r["motivo"]: r["count"] for r in pipeline.table("industrial.silver_descartes").groupBy("motivo").count().collect()}
    assert obtido == motivos_esperados()
    sim10 = pipeline.table("industrial.silver_descartes").where("maquina_id = 'SIM-10'").select("motivo").first()
    assert sim10.motivo == "tipo_invalido"


def test_silver_conserva_cada_linha_da_bronze(pipeline):
    n_bronze = pipeline.table("industrial.bronze_leituras").count()
    n_validas = pipeline.table("industrial.silver_leituras").count()
    n_desc = pipeline.table("industrial.silver_descartes").count()
    assert n_bronze == N_AI4I + len(SIMULADO)
    assert n_validas + n_desc == n_bronze
    assert n_validas == N_AI4I + sum(motivo is None for *_, motivo in SIMULADO)


def test_silver_abre_um_registro_por_modo(pipeline):
    falhas = pipeline.table("industrial.silver_falhas")
    sim01 = sorted(r.modo for r in falhas.where("maquina_id = 'SIM-01'").collect())
    assert sim01 == ["HDF", "PWF"]
    assert falhas.where("maquina_id = 'SIM-03'").select("modo").first().modo == "DESCONHECIDO"
    assert falhas.where("maquina_id = 'AI4I-01'").select("modo").first().modo == "HDF"
    assert falhas.count() == 4


def test_silver_recorta_espacos_e_aceita_o_limite_da_faixa(pipeline):
    bronze = pipeline.table("industrial.bronze_leituras")
    assert bronze.where(F.col("maquina_id") == "  SIM-02  ").count() == 1
    leitura = pipeline.table("industrial.silver_leituras").where("maquina_id = 'SIM-02'").first()
    assert leitura.tipo_produto == "L"
    assert leitura.rotacao_rpm == 0
    assert leitura.desgaste_min == 0
    assert leitura.falha is False
    assert float(leitura.temp_ar_k) == pytest.approx(250.0)
    assert float(leitura.temp_processo_k) == pytest.approx(400.0)
    assert float(leitura.torque_nm) == pytest.approx(0.0)


def test_gold_estrela_converte_celsius_e_agrega_falhas(pipeline):
    leituras = pipeline.table("industrial.gold_fct_leituras")
    dim = pipeline.table("industrial.gold_dim_maquina")
    diarias = pipeline.table("industrial.gold_fct_falhas_diarias")
    silver = pipeline.table("industrial.silver_leituras")

    assert leituras.count() == silver.count()
    assert leituras.select("id_leitura").distinct().count() == leituras.count()
    assert leituras.join(dim, "id_maquina", "left_anti").count() == 0

    sim02 = leituras.where("id_maquina = 'SIM-02'").first()
    assert float(sim02.temperatura_ar_c) == pytest.approx(-23.15)
    assert float(sim02.temperatura_processo_c) == pytest.approx(126.85)
    assert sim02.houve_falha is False
    assert sim02.qtd_modos_falha == 0

    sim01 = leituras.where("id_maquina = 'SIM-01' AND houve_falha").first()
    assert sim01.modos_falha == "HDF, PWF"
    assert sim01.qtd_modos_falha == 2
    assert float(sim01.temperatura_ar_c) == pytest.approx(26.85)
    assert float(sim01.temperatura_processo_c) == pytest.approx(36.85)

    ai4i = leituras.where("id_maquina = 'AI4I-02'").first()
    assert float(ai4i.temperatura_ar_c) == pytest.approx(25.05)
    assert float(ai4i.temperatura_processo_c) == pytest.approx(35.55)
    assert ai4i.rotacao_rpm == 1408
    assert ai4i.desgaste_min == 3
    assert float(ai4i.torque_nm) == pytest.approx(46.3)

    totais = {r.id_maquina: (r.total_leituras, r.total_falhas, r.fonte) for r in dim.collect()}
    assert totais["AI4I-01"] == (2, 1, "ai4i")
    assert totais["AI4I-02"] == (1, 0, "ai4i")
    assert totais["SIM-01"] == (2, 1, "simulado")
    assert totais["SIM-02"] == (1, 0, "simulado")
    assert totais["SIM-03"] == (1, 1, "simulado")
    assert all(r.carregada_em is not None for r in dim.collect())

    soma = diarias.agg(F.sum("qtd_falhas")).first()[0]
    com_falha = leituras.where("houve_falha").count()
    assert soma == com_falha == 3
    por_dia = {(r.id_maquina, str(r.data_ref), r.qtd_falhas, r.modos_falha) for r in diarias.collect()}
    assert ("AI4I-01", "2020-01-01", 1, "HDF") in por_dia
    assert ("SIM-01", "2026-01-01", 1, "HDF, PWF") in por_dia
    assert ("SIM-03", "2026-01-01", 1, "DESCONHECIDO") in por_dia

    for nome in ("silver_leituras", "silver_descartes", "silver_falhas", "gold_fct_leituras", "gold_dim_maquina"):
        formato = pipeline.sql(f"DESCRIBE DETAIL industrial.{nome}").select("format").first().format
        assert formato == "delta", nome


def test_cargas_registra_a_execucao_em_delta(pipeline):
    cargas = pipeline.table("industrial.cargas")
    formato = pipeline.sql("DESCRIBE DETAIL industrial.cargas").select("format").first().format
    assert formato == "delta"
    linha = cargas.first()
    assert linha.linhas_ai4i == N_AI4I
    assert linha.linhas_simulado == len(SIMULADO)
    assert linha.linhas_bronze == N_AI4I + len(SIMULADO)
    assert linha.id_carga
    assert linha.carregado_em is not None
