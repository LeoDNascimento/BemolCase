DROP TABLE IF EXISTS silver_transacoes;

CREATE TABLE silver_transacoes (
    "ID_PESSOA"             BIGINT,
    "DATA"                  DATE,
    "PRODUTO"               TEXT,
    "PRODUTO_PADRONIZADO"   TEXT,
    "VALOR_TRANSACAO"       NUMERIC(18,2),
    "NUM_LOJA"              NUMERIC,
    "INGEST_DATE"           DATE
);
