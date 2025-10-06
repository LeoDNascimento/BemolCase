DROP MATERIALIZED VIEW IF EXISTS gold_cubo_clientes CASCADE;

CREATE MATERIALIZED VIEW gold_cubo_clientes AS
SELECT
    c."ID_PESSOA",
    c."CIDADE_PADRONIZADA" AS "CIDADE",
    c."NIVEL_CLIENTE" as "NIVEL_CLIENTE",

    CURRENT_DATE - MAX(t."DATA") AS recencia_dias,
    COUNT(*)                     AS frequencia,
    SUM(t."VALOR_TRANSACAO")     AS valor_total,

    /* Scores */
    CASE 
        WHEN CURRENT_DATE - MAX(t."DATA") <= 30 THEN 5
        WHEN CURRENT_DATE - MAX(t."DATA") <= 60 THEN 4
        WHEN CURRENT_DATE - MAX(t."DATA") <= 120 THEN 3
        WHEN CURRENT_DATE - MAX(t."DATA") <= 240 THEN 2
        ELSE 1
    END AS r_score,

    CASE 
        WHEN COUNT(*) >= 10 THEN 5
        WHEN COUNT(*) >= 6 THEN 4
        WHEN COUNT(*) >= 3 THEN 3
        WHEN COUNT(*) >= 1 THEN 2
        ELSE 1
    END AS f_score,

    CASE 
        WHEN SUM(t."VALOR_TRANSACAO") >= 1000 THEN 5
        WHEN SUM(t."VALOR_TRANSACAO") >= 500 THEN 4
        WHEN SUM(t."VALOR_TRANSACAO") >= 200 THEN 3
        WHEN SUM(t."VALOR_TRANSACAO") >= 50 THEN 2
        ELSE 1
    END AS v_score,

    /* Soma total e cluster */
    ( 
        CASE 
            WHEN CURRENT_DATE - MAX(t."DATA") <= 30 THEN 5
            WHEN CURRENT_DATE - MAX(t."DATA") <= 60 THEN 4
            WHEN CURRENT_DATE - MAX(t."DATA") <= 120 THEN 3
            WHEN CURRENT_DATE - MAX(t."DATA") <= 240 THEN 2
            ELSE 1
        END 
      + CASE 
            WHEN COUNT(*) >= 10 THEN 5
            WHEN COUNT(*) >= 6 THEN 4
            WHEN COUNT(*) >= 3 THEN 3
            WHEN COUNT(*) >= 1 THEN 2
            ELSE 1
        END
      + CASE 
            WHEN SUM(t."VALOR_TRANSACAO") >= 1000 THEN 5
            WHEN SUM(t."VALOR_TRANSACAO") >= 500 THEN 4
            WHEN SUM(t."VALOR_TRANSACAO") >= 200 THEN 3
            WHEN SUM(t."VALOR_TRANSACAO") >= 50 THEN 2
            ELSE 1
        END
    ) AS rfv_score,

    CASE 
        WHEN (
            CASE 
                WHEN CURRENT_DATE - MAX(t."DATA") <= 30 THEN 5
                WHEN CURRENT_DATE - MAX(t."DATA") <= 60 THEN 4
                WHEN CURRENT_DATE - MAX(t."DATA") <= 120 THEN 3
                WHEN CURRENT_DATE - MAX(t."DATA") <= 240 THEN 2
                ELSE 1
            END
          + CASE 
                WHEN COUNT(*) >= 10 THEN 5
                WHEN COUNT(*) >= 6 THEN 4
                WHEN COUNT(*) >= 3 THEN 3
                WHEN COUNT(*) >= 1 THEN 2
                ELSE 1
            END
          + CASE 
                WHEN SUM(t."VALOR_TRANSACAO") >= 1000 THEN 5
                WHEN SUM(t."VALOR_TRANSACAO") >= 500 THEN 4
                WHEN SUM(t."VALOR_TRANSACAO") >= 200 THEN 3
                WHEN SUM(t."VALOR_TRANSACAO") >= 50 THEN 2
                ELSE 1
            END
        ) BETWEEN 13 AND 15 THEN 'Clientes VIP'
        WHEN (
            CASE 
                WHEN CURRENT_DATE - MAX(t."DATA") <= 30 THEN 5
                WHEN CURRENT_DATE - MAX(t."DATA") <= 60 THEN 4
                WHEN CURRENT_DATE - MAX(t."DATA") <= 120 THEN 3
                WHEN CURRENT_DATE - MAX(t."DATA") <= 240 THEN 2
                ELSE 1
            END
          + CASE 
                WHEN COUNT(*) >= 10 THEN 5
                WHEN COUNT(*) >= 6 THEN 4
                WHEN COUNT(*) >= 3 THEN 3
                WHEN COUNT(*) >= 1 THEN 2
                ELSE 1
            END
          + CASE 
                WHEN SUM(t."VALOR_TRANSACAO") >= 1000 THEN 5
                WHEN SUM(t."VALOR_TRANSACAO") >= 500 THEN 4
                WHEN SUM(t."VALOR_TRANSACAO") >= 200 THEN 3
                WHEN SUM(t."VALOR_TRANSACAO") >= 50 THEN 2
                ELSE 1
            END
        ) BETWEEN 10 AND 12 THEN 'Engajados'
        WHEN (
            CASE 
                WHEN CURRENT_DATE - MAX(t."DATA") <= 30 THEN 5
                WHEN CURRENT_DATE - MAX(t."DATA") <= 60 THEN 4
                WHEN CURRENT_DATE - MAX(t."DATA") <= 120 THEN 3
                WHEN CURRENT_DATE - MAX(t."DATA") <= 240 THEN 2
                ELSE 1
            END
          + CASE 
                WHEN COUNT(*) >= 10 THEN 5
                WHEN COUNT(*) >= 6 THEN 4
                WHEN COUNT(*) >= 3 THEN 3
                WHEN COUNT(*) >= 1 THEN 2
                ELSE 1
            END
          + CASE 
                WHEN SUM(t."VALOR_TRANSACAO") >= 1000 THEN 5
                WHEN SUM(t."VALOR_TRANSACAO") >= 500 THEN 4
                WHEN SUM(t."VALOR_TRANSACAO") >= 200 THEN 3
                WHEN SUM(t."VALOR_TRANSACAO") >= 50 THEN 2
                ELSE 1
            END
        ) BETWEEN 7 AND 9 THEN 'Em risco'
        WHEN (
            CASE 
                WHEN CURRENT_DATE - MAX(t."DATA") <= 30 THEN 5
                WHEN CURRENT_DATE - MAX(t."DATA") <= 60 THEN 4
                WHEN CURRENT_DATE - MAX(t."DATA") <= 120 THEN 3
                WHEN CURRENT_DATE - MAX(t."DATA") <= 240 THEN 2
                ELSE 1
            END
          + CASE 
                WHEN COUNT(*) >= 10 THEN 5
                WHEN COUNT(*) >= 6 THEN 4
                WHEN COUNT(*) >= 3 THEN 3
                WHEN COUNT(*) >= 1 THEN 2
                ELSE 1
            END
          + CASE 
                WHEN SUM(t."VALOR_TRANSACAO") >= 1000 THEN 5
                WHEN SUM(t."VALOR_TRANSACAO") >= 500 THEN 4
                WHEN SUM(t."VALOR_TRANSACAO") >= 200 THEN 3
                WHEN SUM(t."VALOR_TRANSACAO") >= 50 THEN 2
                ELSE 1
            END
        ) BETWEEN 4 AND 6 THEN 'Adormecidos'
        ELSE 'Inativos'
    END AS cluster_rfv

FROM silver_clientes c
LEFT JOIN silver_transacoes t
       ON t."ID_PESSOA" = c."ID_PESSOA"
WHERE t."VALOR_TRANSACAO" > 0
GROUP BY c."ID_PESSOA", c."CIDADE_PADRONIZADA";
