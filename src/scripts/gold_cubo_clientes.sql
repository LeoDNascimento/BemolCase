DROP MATERIALIZED VIEW IF EXISTS gold_cubo_clientes CASCADE;

CREATE MATERIALIZED VIEW gold_cubo_clientes AS
SELECT
    c."ID_PESSOA"                                        AS "ID_PESSOA",
    c."CIDADE_PADRONIZADA"                               AS "CIDADE",
    c."NIVEL_CLIENTE"::NUMERIC(6,0)                      AS "NIVEL_CLIENTE",
    c."DATA_ALTERACAO_NIVEL"                             AS "DATA_ALTERACAO_NIVEL",

    /* métricas */
    COALESCE(SUM(t."VALOR_TRANSACAO"), 0)::NUMERIC(18,2)                                            AS "VOLUME_TOTAL",
    COALESCE(SUM(t."VALOR_TRANSACAO") FILTER (WHERE t."PRODUTO_PADRONIZADO" = 'Recarga Digital'),0) AS "VOLUME_RECARGA",
    COALESCE(SUM(t."VALOR_TRANSACAO") FILTER (WHERE t."PRODUTO_PADRONIZADO" = 'Vale Pre Pago'), 0)  AS "VOLUME_PRE_PAGO"
FROM
    silver_clientes c
LEFT JOIN
    silver_transacoes t
    ON t."ID_PESSOA" = c."ID_PESSOA"
WHERE
1=1
   and t."VALOR_TRANSACAO" > 0     -- exclui negativas
   OR t."ID_PESSOA" IS NULL       -- preserva clientes sem transações
GROUP BY
    c."ID_PESSOA",
    c."CIDADE_PADRONIZADA",
    c."NIVEL_CLIENTE",
    c."DATA_ALTERACAO_NIVEL";
