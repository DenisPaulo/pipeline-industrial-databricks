# Dados

## `ai4i2020.csv`

Cópia do **AI4I 2020 Predictive Maintenance Dataset** (10.000 linhas), exatamente como vem do UCI.

Está no repositório porque o **Databricks Free Edition restringe o acesso à internet**: o notebook
`00_setup` tenta baixar do UCI e, se não conseguir, copia este arquivo para o volume.

- Fonte: UCI Machine Learning Repository, dataset 601
- Link: https://archive.ics.uci.edu/dataset/601/ai4i+2020+predictive+maintenance+dataset
- Licença: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) (permite redistribuir com citação)

> AI4I 2020 Predictive Maintenance Dataset [Dataset]. (2020). UCI Machine Learning Repository. https://doi.org/10.24432/C5HS5C

## Leituras simuladas

Não ficam no repositório: o `00_setup` gera com `notebooks/simulador.py` (semente 42), o mesmo
simulador do repositório base. Mesma semente = mesmo arquivo.
