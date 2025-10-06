DROP TABLE IF EXISTS silver_clientes;

CREATE TABLE silver_clientes (
    "ID_PESSOA"             BIGINT PRIMARY KEY,
    "CIDADE"                TEXT,
    "NIVEL_CLIENTE"         NUMERIC(6,0),
    "DATA_ALTERACAO_NIVEL"  DATE,
    "INGEST_DATE"           DATE,
    "CIDADE_PADRONIZADA"    TEXT,
    "CITY_MATCH_KEY"        TEXT,
    "CITY_MATCH_SCORE"      NUMERIC(6,2)
);
