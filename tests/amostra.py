"""CSV pequeno para os testes bronze → silver → gold.

Cada linha do simulado traz o motivo esperado na silver (None = linha válida).
Os valores seguem as regras de notebooks/_config.py.
"""

from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

COLUNAS = [
    "fonte",
    "maquina_id",
    "timestamp",
    "tipo",
    "temp_ar_k",
    "temp_processo_k",
    "rotacao_rpm",
    "torque_nm",
    "desgaste_min",
    "falha",
    "modo_falha",
]

# motivo None: a silver conserva a linha.
SIMULADO: list[tuple] = [
    ("SIM-01", "2026-01-01 00:00:00", "L", "300.0", "310.0", "1500", "40.0", "10", "0", "", None),
    ("SIM-01", "2026-01-01 00:05:00", "M", "300.0", "310.0", "1500", "40.0", "20", "1", "PWF|HDF", None),
    ("SIM-03", "2026-01-01 00:10:00", "H", "300.0", "310.0", "1500", "40.0", "5", "1", "", None),
    ("SIM-03", "2026-01-01 00:10:00", "H", "300.0", "310.0", "1500", "40.0", "5", "1", "", "duplicada"),
    ("", "2026-01-01 00:15:00", "L", "300.0", "310.0", "1500", "40.0", "5", "0", "", "maquina_ausente"),
    ("SIM-04", "31/01/2026 00:00:00", "L", "300.0", "310.0", "1500", "40.0", "5", "0", "", "timestamp_invalido"),
    ("SIM-05", "2026-01-01 00:20:00", "X", "300.0", "310.0", "1500", "40.0", "5", "0", "", "tipo_invalido"),
    ("SIM-06", "2026-01-01 00:25:00", "L", "300.0", "310.0", "1500", "40.0", "5", "2", "", "falha_invalida"),
    ("SIM-07", "2026-01-01 00:30:00", "L", "", "310.0", "1500", "40.0", "5", "0", "", "sensor_nulo"),
    ("SIM-08", "2026-01-01 00:35:00", "L", "300.0", "310.0", "1500", "ERRO", "5", "0", "", "sensor_nao_numerico"),
    ("SIM-09", "2026-01-01 00:40:00", "L", "999.9", "310.0", "1500", "40.0", "5", "0", "", "fora_da_faixa_fisica"),
    # MOTIVOS: tipo_invalido antecede sensor_nulo.
    ("SIM-10", "2026-01-01 00:45:00", "Z", "", "310.0", "1500", "40.0", "5", "0", "", "tipo_invalido"),
    # Espaços no texto e limites inclusivos das faixas (250 K, 400 K, rotação 0).
    (
        "  SIM-02  ",
        "  2026-01-01 01:00:00  ",
        "  L  ",
        "  250.0  ",
        "  400.0  ",
        "  0  ",
        "  0.0  ",
        "  0  ",
        "  0  ",
        "",
        None,
    ),
]

AI4I_CSV = (
    "UDI,Product ID,Type,Air temperature [K],Process temperature [K],"
    "Rotational speed [rpm],Torque [Nm],Tool wear [min],Machine failure,TWF,HDF,PWF,OSF,RNF\n"
    "1,M14860,M,298.1,308.6,1551,42.8,0,0,0,0,0,0,0\n"
    "2,L47181,L,298.2,308.7,1408,46.3,3,0,0,0,0,0,0\n"
    "11,L47191,L,298.3,308.8,1450,40.0,10,1,0,1,0,0,0\n"
)

N_AI4I = 3


def motivos_esperados() -> dict[str, int]:
    return dict(Counter(motivo for *_, motivo in SIMULADO if motivo))


def gravar(pasta_volume: Path) -> None:
    ai4i = pasta_volume / "ai4i"
    sim = pasta_volume / "simulado"
    ai4i.mkdir(parents=True, exist_ok=True)
    sim.mkdir(parents=True, exist_ok=True)
    (ai4i / "ai4i2020.csv").write_text(AI4I_CSV, encoding="utf-8")
    with (sim / "leituras_simuladas.csv").open("w", encoding="utf-8", newline="") as arquivo:
        escritor = csv.writer(arquivo)
        escritor.writerow(COLUNAS)
        for linha in SIMULADO:
            escritor.writerow(["simulado", *linha[:-1]])
