-- total (cluster final do cliente)
SELECT cluster_rfv_total AS cluster, COUNT(DISTINCT "ID_PESSOA") AS qtde_clientes
FROM gold_clientes_rfv_produto
GROUP BY cluster_rfv_total
ORDER BY qtde_clientes DESC;

-- por produto (cluster por produto)
SELECT "PRODUTO", cluster_rfv_prod AS cluster, COUNT(DISTINCT "ID_PESSOA") AS qtde_clientes
FROM gold_clientes_rfv_produto
GROUP BY "PRODUTO", cluster_rfv_prod
ORDER BY "PRODUTO", qtde_clientes DESC;

-- Volume por produto e cluster (quem movimenta mais)
SELECT
  "PRODUTO",
  cluster_rfv_prod AS cluster,
  SUM(valor_total_prod) AS volume_total
FROM gold_clientes_rfv_produto
GROUP BY "PRODUTO", cluster_rfv_prod
ORDER BY "PRODUTO", volume_total DESC;

--Retenção e risco: recência média por cluster (tendência de churn) 

SELECT
  cluster_rfv_total AS cluster,
  AVG(recencia_dias_total) AS recencia_media_dias
FROM gold_clientes_rfv_produto
GROUP BY cluster_rfv_total
ORDER BY recencia_media_dias;  -- menor = mais recente/ativo

-- Cross-sell: clientes com mais de 1 produto ativo

-- contagem de clientes por nº de produtos
WITH produtos_por_cliente AS (
  SELECT "ID_PESSOA", COUNT(DISTINCT "PRODUTO") AS n_produtos
  FROM gold_clientes_rfv_produto
  WHERE valor_total_prod > 0
  GROUP BY "ID_PESSOA"
)
SELECT
  n_produtos,
  COUNT(*) AS qtde_clientes
FROM produtos_por_cliente
GROUP BY n_produtos
ORDER BY n_produtos DESC;

-- proporção de clientes com 2 produtos (oportunidade)
WITH p AS (
  SELECT "ID_PESSOA", COUNT(DISTINCT "PRODUTO") AS n_produtos
  FROM gold_clientes_rfv_produto
  WHERE valor_total_prod > 0
  GROUP BY "ID_PESSOA"
)
SELECT
  SUM(CASE WHEN n_produtos >= 2 THEN 1 ELSE 0 END)::decimal / COUNT(*)*100 AS proporcao_mult_produto
FROM p;

-- rfv médio
SELECT
  "PRODUTO",
  AVG(rfv_score_prod)  AS rfv_medio_produto,
  AVG(rfv_score_total) AS rfv_medio_total
FROM gold_clientes_rfv_produto
GROUP BY "PRODUTO"
ORDER BY rfv_medio_produto DESC;

-- Clientes em risco: quem reativar primeiro (priorização)
WITH ranked AS (
  SELECT
    "ID_PESSOA",
    "CIDADE",
    recencia_dias_total,
    frequencia_total,
    valor_total_geral,
    cluster_rfv_total,
    ROW_NUMBER() OVER (PARTITION BY "ID_PESSOA" ORDER BY valor_total_geral DESC) AS rn
  FROM gold_clientes_rfv_produto
)
SELECT
  "ID_PESSOA",
  "CIDADE",
  recencia_dias_total,
  frequencia_total,
  valor_total_geral
FROM ranked
WHERE rn = 1
  AND cluster_rfv_total IN ('Em risco', 'Adormecidos')
ORDER BY recencia_dias_total DESC, valor_total_geral DESC
LIMIT 100;


-- relação entre frequência e valor (por produto)
SELECT
  "PRODUTO",
  CORR(frequencia_prod::float, valor_total_prod::float) AS corr_freq_valor
FROM gold_clientes_rfv_produto
GROUP BY "PRODUTO";


-- Conta quem usou só Recarga, só Pré-Pago, ambos e nenhum (clientes sem transações positivas).
WITH base AS (
  SELECT DISTINCT
         t."ID_PESSOA",
         t."PRODUTO_PADRONIZADO"
  FROM silver_transacoes t
  WHERE t."VALOR_TRANSACAO" > 0
    AND t."PRODUTO_PADRONIZADO" IN ('Recarga Digital','Vale Pre Pago')
),
flags AS (
  SELECT
    c."ID_PESSOA",
    MAX(CASE WHEN b."PRODUTO_PADRONIZADO" = 'Recarga Digital' THEN 1 ELSE 0 END) AS has_recarga,
    MAX(CASE WHEN b."PRODUTO_PADRONIZADO" = 'Vale Pre Pago'   THEN 1 ELSE 0 END) AS has_pre
  FROM silver_clientes c
  LEFT JOIN base b ON b."ID_PESSOA" = c."ID_PESSOA"
  GROUP BY c."ID_PESSOA"
)
SELECT
  SUM( (has_recarga=1 AND has_pre=0)::int ) AS somente_recarga,
  SUM( (has_recarga=0 AND has_pre=1)::int ) AS somente_pre_pago,
  SUM( (has_recarga=1 AND has_pre=1)::int ) AS ambos,
  SUM( (has_recarga=0 AND has_pre=0)::int ) AS nenhum,
  COUNT(*)                                   AS total_clientes,
  ROUND( 100.0 * SUM( (has_recarga=1 AND has_pre=1)::int ) / NULLIF(COUNT(*),0), 2) AS pct_ambos
FROM flags;




-- Valor transacionado por "PRODUTO"
SELECT 
"PRODUTO", 
SUM(valor_total_prod) AS volume_total ,
AVG(f_score_prod) as avg_f
FROM gold_clientes_rfv_produto 
GROUP BY "PRODUTO"
