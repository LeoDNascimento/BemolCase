SELECT
  (SELECT COUNT(*) FROM gold_cubo_clientes) AS cubo_rows,
  (SELECT COUNT(*) FROM silver_clientes)    AS clientes_rows;

SELECT "ID_PESSOA", COUNT(*) AS n
  FROM gold_cubo_clientes
  GROUP BY "ID_PESSOA"
  HAVING COUNT(*) > 1;

-- registros com campos críticos nulos
SELECT
  SUM(CASE WHEN "ID_PESSOA" IS NULL THEN 1 ELSE 0 END) AS n_id_pessoa_null,
  SUM(CASE WHEN "CIDADE" IS NULL THEN 1 ELSE 0 END)     AS n_cidade_null,
  SUM(CASE WHEN "NIVEL_CLIENTE" IS NULL THEN 1 ELSE 0 END) AS n_nivel_null
FROM gold_cubo_clientes;

-- volumes negativos (deve ser 0)
SELECT
  SUM(CASE WHEN "VOLUME_TOTAL"    < 0 THEN 1 ELSE 0 END) AS neg_total,
  SUM(CASE WHEN "VOLUME_RECARGA"  < 0 THEN 1 ELSE 0 END) AS neg_recarga,
  SUM(CASE WHEN "VOLUME_PRE_PAGO" < 0 THEN 1 ELSE 0 END) AS neg_pre_pago
FROM gold_cubo_clientes;


-- transações sem cliente correspondente (idealmente 0)
SELECT COUNT(*) AS transacoes_orfas
FROM silver_transacoes t
LEFT JOIN silver_clientes c ON c."ID_PESSOA" = t."ID_PESSOA"
WHERE c."ID_PESSOA" IS NULL;