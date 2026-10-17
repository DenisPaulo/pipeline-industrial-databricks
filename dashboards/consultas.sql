-- Consultas do AI/BI Dashboard "pipeline-industrial" (Databricks Free Edition).
-- Cada bloco é um dataset: no dashboard, aba Data > Create from SQL > cole UMA consulta por vez
-- (sem o ";" final, se preferir). As tabelas são criadas pelos notebooks 01 a 03.
-- Catálogo e schema padrão: workspace.industrial (mude aqui se usou outro em notebooks/_config).

-- [1] kpis — Gráfico: 4 widgets "Counter" (um para cada coluna)
SELECT
  COUNT(*)                                                AS total_leituras,
  COUNT_IF(houve_falha)                                   AS leituras_com_falha,
  ROUND(100.0 * COUNT_IF(houve_falha) / COUNT(*), 2)      AS taxa_falha_pct,
  COUNT(DISTINCT id_maquina)                              AS maquinas
FROM workspace.industrial.gold_fct_leituras;

-- [2] falhas_por_dia — Gráfico: "Bar" (X = data_ref, Y = falhas, Color = fonte)
-- Obs.: os dados AI4I caem em jan/2020 e os simulados em jan/2026 (datas didáticas).
SELECT
  f.data_ref,
  d.fonte,
  SUM(f.qtd_falhas) AS falhas
FROM workspace.industrial.gold_fct_falhas_diarias AS f
JOIN workspace.industrial.gold_dim_maquina AS d
  ON d.id_maquina = f.id_maquina
GROUP BY f.data_ref, d.fonte
ORDER BY f.data_ref, d.fonte;

-- [3] top10_maquinas — Gráfico: "Bar" horizontal (Y = id_maquina, X = total_falhas, Color = fonte)
SELECT
  id_maquina,
  fonte,
  total_falhas,
  total_leituras,
  ROUND(100.0 * total_falhas / total_leituras, 2) AS taxa_falha_pct
FROM workspace.industrial.gold_dim_maquina
ORDER BY total_falhas DESC, id_maquina
LIMIT 10;

-- [4] falhas_por_modo — Gráfico: "Pie" ou "Bar" (X = modo_falha, Y = registros)
-- Uma leitura pode ter mais de um modo; DESCONHECIDO = falha sem modo informado.
SELECT
  modo AS modo_falha,
  COUNT(*) AS registros
FROM workspace.industrial.silver_falhas
GROUP BY modo
ORDER BY registros DESC, modo_falha;

-- [5] descartes_por_motivo — Gráfico: "Bar" (X = motivo, Y = linhas)
SELECT
  motivo,
  COUNT(*) AS linhas,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_dos_descartes
FROM workspace.industrial.silver_descartes
GROUP BY motivo
ORDER BY linhas DESC, motivo;

-- [6] ultima_carga — Gráfico: "Counter" (horas_desde_carga) ou "Table"
SELECT
  carregado_em,
  linhas_bronze,
  linhas_ai4i,
  linhas_simulado,
  ROUND((unix_timestamp(current_timestamp()) - unix_timestamp(carregado_em)) / 3600, 1) AS horas_desde_carga
FROM workspace.industrial.cargas
ORDER BY carregado_em DESC
LIMIT 1;
