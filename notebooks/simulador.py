"""Gerador de leituras simuladas de sensores (séries por máquina), com semente fixa.

Cada máquina tem uma série temporal com ciclo diário de temperatura, ruído, desgaste de
ferramenta com trocas e falhas calculadas por regras físicas simplificadas (no estilo do AI4I).
Depois, injeta problemas de qualidade de propósito para exercitar a limpeza:
valores nulos, outliers (erro de unidade/sensor), texto inválido e linhas duplicadas.

Uso no Databricks: o notebook 00_setup importa ``gerar_leituras`` deste arquivo.
Uso local:         python notebooks/simulador.py --semente 42 --saida /tmp/sim.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

# Cópia do simulador do repositório base (pipeline-dados-industriais), sem dependências
# do pacote original: as constantes abaixo vêm de src/pipeline/schemas.py.
SENSORES = ["temp_ar_k", "temp_processo_k", "rotacao_rpm", "torque_nm", "desgaste_min"]
COLUNAS_BRUTO = ["fonte", "maquina_id", "timestamp", "tipo", *SENSORES, "falha", "modo_falha"]

LIMITE_OSF = {"L": 11_000, "M": 12_000, "H": 13_000}
INICIO_PADRAO = "2026-01-01 00:00:00"

# Probabilidades de injeção de problemas (por célula de sensor, exceto duplicadas: por linha)
P_NULO = 0.015
P_OUTLIER = 0.004
P_LIXO = 0.002
P_DUPLICADA = 0.01


def _serie_maquina(rng: np.random.Generator, n: int, intervalo_min: int) -> pd.DataFrame:
    t = np.arange(n)
    passos_dia = 24 * 60 / intervalo_min
    fase = rng.uniform(0, 2 * np.pi)
    ar = 300 + rng.uniform(-1, 1) + 1.5 * np.sin(2 * np.pi * t / passos_dia + fase)
    ar = ar + rng.normal(0, 0.25, n)
    processo = ar + 10 + rng.normal(0, 0.8, n)
    rpm = np.round(1500 + rng.normal(0, 150, n))
    torque = np.clip(40 - 0.02 * (rpm - 1500) + rng.normal(0, 5, n), 1, None)
    tipo = rng.choice(["L", "M", "H"], size=n, p=[0.6, 0.3, 0.1])

    desgaste = np.zeros(n)
    twf = np.zeros(n, dtype=bool)
    w, limite = int(rng.integers(0, 100)), int(rng.integers(200, 241))
    for i in range(n):
        w += int(rng.integers(1, 4))
        if w >= limite:
            twf[i] = rng.random() < 0.5  # metade das trocas acontece só depois da quebra
            desgaste[i] = w
            w, limite = 0, int(rng.integers(200, 241))
        else:
            desgaste[i] = w

    potencia = torque * rpm * 2 * np.pi / 60
    hdf = ((processo - ar) < 8.6) & (rpm < 1380)
    pwf = (potencia < 3500) | (potencia > 9000)
    osf = desgaste * torque > np.vectorize(LIMITE_OSF.get)(tipo)
    rnf = rng.random(n) < 0.001

    modos = np.stack([twf, hdf, pwf, osf, rnf], axis=1)
    nomes = np.array(["TWF", "HDF", "PWF", "OSF", "RNF"])
    modo_txt = ["|".join(nomes[linha]) for linha in modos]
    return pd.DataFrame(
        {
            "tipo": tipo,
            "temp_ar_k": np.round(ar, 1),
            "temp_processo_k": np.round(processo, 1),
            "rotacao_rpm": rpm.astype(int),
            "torque_nm": np.round(torque, 1),
            "desgaste_min": desgaste.astype(int),
            "falha": modos.any(axis=1).astype(int),
            "modo_falha": [m or None for m in modo_txt],
        }
    )


def _candidatos_outlier(coluna: str, valor: str) -> list[str]:
    if coluna in ("temp_ar_k", "temp_processo_k"):
        em_celsius = f"{float(valor) - 273.15:.1f}" if valor not in ("", None) else "25.0"
        return ["0.0", "999.9", em_celsius]  # zero, saturação e "esqueceu de converter °C -> K"
    if coluna == "rotacao_rpm":
        return ["-1", "65535"]
    if coluna == "torque_nm":
        return ["-5.0", "999.0"]
    return ["-1", "99999"]


def gerar_leituras(
    semente: int = 42,
    n_maquinas: int = 8,
    dias: int = 7,
    intervalo_min: int = 5,
    inicio: str = INICIO_PADRAO,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Gera as leituras (tudo como texto) e um resumo do que foi injetado.

    Mesma semente e mesmos parâmetros => saída idêntica.
    """
    rng = np.random.default_rng(semente)
    n = int(dias * 24 * 60 / intervalo_min)
    base = pd.Timestamp(inicio)
    quadros = []
    for k in range(1, n_maquinas + 1):
        serie = _serie_maquina(rng, n, intervalo_min)
        serie.insert(0, "timestamp", (base + pd.to_timedelta(np.arange(n) * intervalo_min, unit="min")))
        serie.insert(0, "maquina_id", f"SIM-{k:02d}")
        quadros.append(serie)
    df = pd.concat(quadros, ignore_index=True)
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["fonte"] = "simulado"
    for col in ["temp_ar_k", "temp_processo_k", "torque_nm"]:
        df[col] = df[col].map(lambda v: f"{v:.1f}")
    for col in ["rotacao_rpm", "desgaste_min", "falha"]:
        df[col] = df[col].astype(str)
    df = df[COLUNAS_BRUTO].astype("object")

    resumo = {"linhas_base": len(df), "nulos": 0, "outliers": 0, "lixo": 0, "duplicadas": 0}
    for col in SENSORES:
        sorteio = rng.random(len(df))
        m_nulo = sorteio < P_NULO
        m_out = (sorteio >= P_NULO) & (sorteio < P_NULO + P_OUTLIER)
        m_lixo = (sorteio >= P_NULO + P_OUTLIER) & (sorteio < P_NULO + P_OUTLIER + P_LIXO)
        valores = df[col].to_numpy(dtype=object).copy()
        for i in np.flatnonzero(m_out):
            opcoes = _candidatos_outlier(col, valores[i])
            valores[i] = opcoes[int(rng.integers(0, len(opcoes)))]
        valores[m_lixo] = "ERRO"
        valores[m_nulo] = None
        df[col] = valores
        resumo["nulos"] += int(m_nulo.sum())
        resumo["outliers"] += int(m_out.sum())
        resumo["lixo"] += int(m_lixo.sum())

    dup = df[rng.random(len(df)) < P_DUPLICADA]
    resumo["duplicadas"] = len(dup)
    df = pd.concat([df, dup], ignore_index=True)  # duplicatas "chegam atrasadas", no fim
    resumo["linhas_total"] = len(df)
    return df, resumo


def gravar_csv(df: pd.DataFrame, destino: Path) -> Path:
    destino.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(destino, index=False)
    return destino


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--semente", type=int, default=42)
    p.add_argument("--maquinas", type=int, default=8)
    p.add_argument("--dias", type=int, default=7)
    p.add_argument("--saida", type=Path, default=Path("data/raw/simulado/leituras_simuladas.csv"))
    a = p.parse_args()
    df, resumo = gerar_leituras(a.semente, a.maquinas, a.dias)
    gravar_csv(df, a.saida)
    print(f"[simulador] {a.saida}: {resumo}")


if __name__ == "__main__":
    main()
