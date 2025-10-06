DROP MATERIALIZED VIEW IF EXISTS gold_clientes CASCADE;

CREATE MATERIALIZED VIEW gold_clientes AS
WITH
  params AS (
    SELECT DATE '2024-02-01' AS ref_date
  ),

  -- Agregado por PRODUTO (só valores > 0)
  agg_prod AS (
    SELECT
        t."ID_PESSOA",
        t."PRODUTO_PADRONIZADO"               AS "PRODUTO",
        MAX(t."DATA")                         AS last_date_prod,
        COUNT(*)                              AS freq_prod,
        SUM(t."VALOR_TRANSACAO")              AS val_prod
    FROM silver_transacoes t
    WHERE t."VALOR_TRANSACAO" > 0
    GROUP BY t."ID_PESSOA", t."PRODUTO_PADRONIZADO"
  ),

  -- Agregado TOTAL do cliente (independe de produto; só valores > 0)
  agg_total AS (
    SELECT
        t."ID_PESSOA",
        MAX(t."DATA")                         AS last_date_all,
        COUNT(*)                              AS freq_all,
        SUM(t."VALOR_TRANSACAO")              AS val_all
    FROM silver_transacoes t
    WHERE t."VALOR_TRANSACAO" > 0
    GROUP BY t."ID_PESSOA"
  )

SELECT
    c."ID_PESSOA",
    c."CIDADE_PADRONIZADA"                   AS "CIDADE",
    c."NIVEL_CLIENTE"                         AS "NIVEL_CLIENTE",
    p."PRODUTO"                               AS "PRODUTO",

    /* ===== Métricas por PRODUTO ===== */
    (SELECT ref_date FROM params) - p.last_date_prod          AS recencia_dias_prod,
    COALESCE(p.freq_prod, 0)                                  AS frequencia_prod,
    COALESCE(p.val_prod, 0)::NUMERIC(18,2)                    AS valor_total_prod,

    /* Scores R F V por PRODUTO (1..5) */
    CASE 
      WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 30  THEN 5
      WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 60  THEN 4
      WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 120 THEN 3
      WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 240 THEN 2
      ELSE 1
    END                                                     AS r_score_prod,
    CASE 
      WHEN COALESCE(p.freq_prod,0) >= 10 THEN 5
      WHEN COALESCE(p.freq_prod,0) >= 6  THEN 4
      WHEN COALESCE(p.freq_prod,0) >= 3  THEN 3
      WHEN COALESCE(p.freq_prod,0) >= 1  THEN 2
      ELSE 1
    END                                                     AS f_score_prod,
    CASE 
      WHEN COALESCE(p.val_prod,0) >= 1000 THEN 5
      WHEN COALESCE(p.val_prod,0) >= 500  THEN 4
      WHEN COALESCE(p.val_prod,0) >= 200  THEN 3
      WHEN COALESCE(p.val_prod,0) >= 50   THEN 2
      ELSE 1
    END                                                     AS v_score_prod,

    /* RFV por PRODUTO e Cluster por PRODUTO */
    (
      CASE 
        WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 30  THEN 5
        WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 60  THEN 4
        WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 120 THEN 3
        WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 240 THEN 2
        ELSE 1
      END
      + CASE 
          WHEN COALESCE(p.freq_prod,0) >= 10 THEN 5
          WHEN COALESCE(p.freq_prod,0) >= 6  THEN 4
          WHEN COALESCE(p.freq_prod,0) >= 3  THEN 3
          WHEN COALESCE(p.freq_prod,0) >= 1  THEN 2
          ELSE 1
        END
      + CASE 
          WHEN COALESCE(p.val_prod,0) >= 1000 THEN 5
          WHEN COALESCE(p.val_prod,0) >= 500  THEN 4
          WHEN COALESCE(p.val_prod,0) >= 200  THEN 3
          WHEN COALESCE(p.val_prod,0) >= 50   THEN 2
          ELSE 1
        END
    )                                                       AS rfv_score_prod,

    CASE 
      WHEN (
        CASE 
          WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 30  THEN 5
          WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 60  THEN 4
          WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 120 THEN 3
          WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 240 THEN 2
          ELSE 1
        END
        + CASE 
            WHEN COALESCE(p.freq_prod,0) >= 10 THEN 5
            WHEN COALESCE(p.freq_prod,0) >= 6  THEN 4
            WHEN COALESCE(p.freq_prod,0) >= 3  THEN 3
            WHEN COALESCE(p.freq_prod,0) >= 1  THEN 2
            ELSE 1
          END
        + CASE 
            WHEN COALESCE(p.val_prod,0) >= 1000 THEN 5
            WHEN COALESCE(p.val_prod,0) >= 500  THEN 4
            WHEN COALESCE(p.val_prod,0) >= 200  THEN 3
            WHEN COALESCE(p.val_prod,0) >= 50   THEN 2
            ELSE 1
          END
      ) BETWEEN 13 AND 15 THEN 'Clientes VIP'
      WHEN (
        CASE 
          WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 30  THEN 5
          WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 60  THEN 4
          WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 120 THEN 3
          WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 240 THEN 2
          ELSE 1
        END
        + CASE 
            WHEN COALESCE(p.freq_prod,0) >= 10 THEN 5
            WHEN COALESCE(p.freq_prod,0) >= 6  THEN 4
            WHEN COALESCE(p.freq_prod,0) >= 3  THEN 3
            WHEN COALESCE(p.freq_prod,0) >= 1  THEN 2
            ELSE 1
          END
        + CASE 
            WHEN COALESCE(p.val_prod,0) >= 1000 THEN 5
            WHEN COALESCE(p.val_prod,0) >= 500  THEN 4
            WHEN COALESCE(p.val_prod,0) >= 200  THEN 3
            WHEN COALESCE(p.val_prod,0) >= 50   THEN 2
            ELSE 1
          END
      ) BETWEEN 10 AND 12 THEN 'Engajados'
      WHEN (
        CASE 
          WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 30  THEN 5
          WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 60  THEN 4
          WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 120 THEN 3
          WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 240 THEN 2
          ELSE 1
        END
        + CASE 
            WHEN COALESCE(p.freq_prod,0) >= 10 THEN 5
            WHEN COALESCE(p.freq_prod,0) >= 6  THEN 4
            WHEN COALESCE(p.freq_prod,0) >= 3  THEN 3
            WHEN COALESCE(p.freq_prod,0) >= 1  THEN 2
            ELSE 1
          END
        + CASE 
            WHEN COALESCE(p.val_prod,0) >= 1000 THEN 5
            WHEN COALESCE(p.val_prod,0) >= 500  THEN 4
            WHEN COALESCE(p.val_prod,0) >= 200  THEN 3
            WHEN COALESCE(p.val_prod,0) >= 50   THEN 2
            ELSE 1
          END
      ) BETWEEN 7 AND 9 THEN 'Em risco'
      WHEN (
        CASE 
          WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 30  THEN 5
          WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 60  THEN 4
          WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 120 THEN 3
          WHEN (SELECT ref_date FROM params) - p.last_date_prod <= 240 THEN 2
          ELSE 1
        END
        + CASE 
            WHEN COALESCE(p.freq_prod,0) >= 10 THEN 5
            WHEN COALESCE(p.freq_prod,0) >= 6  THEN 4
            WHEN COALESCE(p.freq_prod,0) >= 3  THEN 3
            WHEN COALESCE(p.freq_prod,0) >= 1  THEN 2
            ELSE 1
          END
        + CASE 
            WHEN COALESCE(p.val_prod,0) >= 1000 THEN 5
            WHEN COALESCE(p.val_prod,0) >= 500  THEN 4
            WHEN COALESCE(p.val_prod,0) >= 200  THEN 3
            WHEN COALESCE(p.val_prod,0) >= 50   THEN 2
            ELSE 1
          END
      ) BETWEEN 4 AND 6 THEN 'Adormecidos'
      ELSE 'Inativos'
    END                                                   AS cluster_rfv_prod,

    /* ===== Métricas TOTAIS do cliente ===== */
    (SELECT ref_date FROM params) - g.last_date_all        AS recencia_dias_total,
    COALESCE(g.freq_all, 0)                                AS frequencia_total,
    COALESCE(g.val_all, 0)::NUMERIC(18,2)                  AS valor_total_geral,

    /* Scores totais (R,F,V) */
    CASE 
      WHEN (SELECT ref_date FROM params) - g.last_date_all <= 30  THEN 5
      WHEN (SELECT ref_date FROM params) - g.last_date_all <= 60  THEN 4
      WHEN (SELECT ref_date FROM params) - g.last_date_all <= 120 THEN 3
      WHEN (SELECT ref_date FROM params) - g.last_date_all <= 240 THEN 2
      ELSE 1
    END                                                   AS r_score_total,
    CASE 
      WHEN COALESCE(g.freq_all,0) >= 10 THEN 5
      WHEN COALESCE(g.freq_all,0) >= 6  THEN 4
      WHEN COALESCE(g.freq_all,0) >= 3  THEN 3
      WHEN COALESCE(g.freq_all,0) >= 1  THEN 2
      ELSE 1
    END                                                   AS f_score_total,
    CASE 
      WHEN COALESCE(g.val_all,0) >= 1000 THEN 5
      WHEN COALESCE(g.val_all,0) >= 500  THEN 4
      WHEN COALESCE(g.val_all,0) >= 200  THEN 3
      WHEN COALESCE(g.val_all,0) >= 50   THEN 2
      ELSE 1
    END                                                   AS v_score_total,

    /* RFV total e cluster final do cliente (na mesma linha) */
    (
      CASE 
        WHEN (SELECT ref_date FROM params) - g.last_date_all <= 30  THEN 5
        WHEN (SELECT ref_date FROM params) - g.last_date_all <= 60  THEN 4
        WHEN (SELECT ref_date FROM params) - g.last_date_all <= 120 THEN 3
        WHEN (SELECT ref_date FROM params) - g.last_date_all <= 240 THEN 2
        ELSE 1
      END
      + CASE 
          WHEN COALESCE(g.freq_all,0) >= 10 THEN 5
          WHEN COALESCE(g.freq_all,0) >= 6  THEN 4
          WHEN COALESCE(g.freq_all,0) >= 3  THEN 3
          WHEN COALESCE(g.freq_all,0) >= 1  THEN 2
          ELSE 1
        END
      + CASE 
          WHEN COALESCE(g.val_all,0) >= 1000 THEN 5
          WHEN COALESCE(g.val_all,0) >= 500  THEN 4
          WHEN COALESCE(g.val_all,0) >= 200  THEN 3
          WHEN COALESCE(g.val_all,0) >= 50   THEN 2
          ELSE 1
        END
    )                                                     AS rfv_score_total,

    CASE 
      WHEN (
        CASE 
          WHEN (SELECT ref_date FROM params) - g.last_date_all <= 30  THEN 5
          WHEN (SELECT ref_date FROM params) - g.last_date_all <= 60  THEN 4
          WHEN (SELECT ref_date FROM params) - g.last_date_all <= 120 THEN 3
          WHEN (SELECT ref_date FROM params) - g.last_date_all <= 240 THEN 2
          ELSE 1
        END
        + CASE 
            WHEN COALESCE(g.freq_all,0) >= 10 THEN 5
            WHEN COALESCE(g.freq_all,0) >= 6  THEN 4
            WHEN COALESCE(g.freq_all,0) >= 3  THEN 3
            WHEN COALESCE(g.freq_all,0) >= 1  THEN 2
            ELSE 1
          END
        + CASE 
            WHEN COALESCE(g.val_all,0) >= 1000 THEN 5
            WHEN COALESCE(g.val_all,0) >= 500  THEN 4
            WHEN COALESCE(g.val_all,0) >= 200  THEN 3
            WHEN COALESCE(g.val_all,0) >= 50   THEN 2
            ELSE 1
          END
      ) BETWEEN 13 AND 15 THEN 'Clientes VIP'
      WHEN (
        CASE 
          WHEN (SELECT ref_date FROM params) - g.last_date_all <= 30  THEN 5
          WHEN (SELECT ref_date FROM params) - g.last_date_all <= 60  THEN 4
          WHEN (SELECT ref_date FROM params) - g.last_date_all <= 120 THEN 3
          WHEN (SELECT ref_date FROM params) - g.last_date_all <= 240 THEN 2
          ELSE 1
        END
        + CASE 
            WHEN COALESCE(g.freq_all,0) >= 10 THEN 5
            WHEN COALESCE(g.freq_all,0) >= 6  THEN 4
            WHEN COALESCE(g.freq_all,0) >= 3  THEN 3
            WHEN COALESCE(g.freq_all,0) >= 1  THEN 2
            ELSE 1
          END
        + CASE 
            WHEN COALESCE(g.val_all,0) >= 1000 THEN 5
            WHEN COALESCE(g.val_all,0) >= 500  THEN 4
            WHEN COALESCE(g.val_all,0) >= 200  THEN 3
            WHEN COALESCE(g.val_all,0) >= 50   THEN 2
            ELSE 1
          END
      ) BETWEEN 10 AND 12 THEN 'Engajados'
      WHEN (
        CASE 
          WHEN (SELECT ref_date FROM params) - g.last_date_all <= 30  THEN 5
          WHEN (SELECT ref_date FROM params) - g.last_date_all <= 60  THEN 4
          WHEN (SELECT ref_date FROM params) - g.last_date_all <= 120 THEN 3
          WHEN (SELECT ref_date FROM params) - g.last_date_all <= 240 THEN 2
          ELSE 1
        END
        + CASE 
            WHEN COALESCE(g.freq_all,0) >= 10 THEN 5
            WHEN COALESCE(g.freq_all,0) >= 6  THEN 4
            WHEN COALESCE(g.freq_all,0) >= 3  THEN 3
            WHEN COALESCE(g.freq_all,0) >= 1  THEN 2
            ELSE 1
          END
        + CASE 
            WHEN COALESCE(g.val_all,0) >= 1000 THEN 5
            WHEN COALESCE(g.val_all,0) >= 500  THEN 4
            WHEN COALESCE(g.val_all,0) >= 200  THEN 3
            WHEN COALESCE(g.val_all,0) >= 50   THEN 2
            ELSE 1
          END
      ) BETWEEN 7 AND 9 THEN 'Em risco'
      WHEN (
        CASE 
          WHEN (SELECT ref_date FROM params) - g.last_date_all <= 30  THEN 5
          WHEN (SELECT ref_date FROM params) - g.last_date_all <= 60  THEN 4
          WHEN (SELECT ref_date FROM params) - g.last_date_all <= 120 THEN 3
          WHEN (SELECT ref_date FROM params) - g.last_date_all <= 240 THEN 2
          ELSE 1
        END
        + CASE 
            WHEN COALESCE(g.freq_all,0) >= 10 THEN 5
            WHEN COALESCE(g.freq_all,0) >= 6  THEN 4
            WHEN COALESCE(g.freq_all,0) >= 3  THEN 3
            WHEN COALESCE(g.freq_all,0) >= 1  THEN 2
            ELSE 1
          END
        + CASE 
            WHEN COALESCE(g.val_all,0) >= 1000 THEN 5
            WHEN COALESCE(g.val_all,0) >= 500  THEN 4
            WHEN COALESCE(g.val_all,0) >= 200  THEN 3
            WHEN COALESCE(g.val_all,0) >= 50   THEN 2
            ELSE 1
          END
      ) BETWEEN 4 AND 6 THEN 'Adormecidos'
      ELSE 'Inativos'
    END                                                   AS cluster_rfv_total

FROM silver_clientes c
LEFT JOIN agg_prod  p ON p."ID_PESSOA" = c."ID_PESSOA"
LEFT JOIN agg_total g ON g."ID_PESSOA" = c."ID_PESSOA";
