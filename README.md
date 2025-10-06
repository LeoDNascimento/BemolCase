#  Desafio Business Analytics — Bemol Serviços Financeiros

##  Visão Geral
Este projeto foi desenvolvido como parte do **case de Business Analytics da Bemol Serviços Financeiros**, com foco em analisar o comportamento e a retenção dos clientes entre dois produtos:  

- 🟪 **Vale Pré-Pago** — produto tradicional, com base ampla e alto valor histórico.  
- 🟧 **Recarga Digital** — produto mais novo, de perfil digital e maior engajamento.  

**Objetivo principal:** entender **como reter clientes antigos e acelerar a migração para o novo produto**, sem perda de receita.

---

## Como Iniciar o Projeto

### 1. Clonar o repositório

```bash
git clone https://github.com/seuusuario/bemol-business-analytics.git
cd bemol-business-analytics
```

### 2. Poetry
Instalar caso não tenha:
```bash
pip install poetry
```

Criar o ambiente virtual e instalar as dependências
```bash
poetry install
poetry shell
```
Rodar Pipeline
```bash
poetry run python src/scripts/main.py
```

### Se não tiver Poetry
```bash
python -m venv .venv
source .venv/bin/activate  # (ou .venv\Scripts\activate no Windows)
pip install -r requirements.txt

```

---
## Estrutura do Projeto

```bash
.
├── data/
│   ├── raw/            # Dados originais (CSV)
│   ├── bronze/        
        ├── landing/    # Dados iniciais de ingestão versionado por data
        ├── validated/  # Dados tratados pré silver
│   ├── silver/         # Tabelas modeladas
│   ├── gold/           # Cubos analíticos
│   └── temp/           # Cache / staging
│
├── dim/
│   ├── canon_cidades.txt/         # Dim para normalizar os nomes das cidades
│
├── Exploratória/
│   ├── Exploratória.sql/         # Scripts sql para exploratória
│
├── scripts/
│   ├── bronze_ingest.py
│   ├── bronze_validate.py
│   ├── bronze_citymatch_clientes.py
│   └── bronze_produtomatch_transacoes.py
```

---
# O projeto (fim-a-fim)
---
### 1. Coloque os CSVs brutos no lugar certo
```bash
src/data/raw/clientes/BemolDesafioProdutosFinanceirosClientes.csv
src/data/raw/transacoes/BemolDesafioProdutosFinanceirosTransacoes.csv

```
Obs: mantenha os nomes ou ajuste o config.yaml.

### 2. Visão do fluxo (do RAW até a GOLD)

```bash
RAW (.csv)
   └─► BRONZE (Python)
        ├─ bronze_ingest.py            → leitura, padronização básica, datas/colunas
        ├─ bronze_validate.py          → sanidade (nulos, duplicados, tipos)
        ├─ bronze_produtomatch_transacoes.py → normaliza PRODUTO (Recarga / Vale Pré-Pago)
        └─ bronze_citymatch_clientes.py→ normaliza CIDADE com RapidFuzz (ex.: Manaus)
                 │
                 └── arquivos .csv tratados:
                     src/data/bronze/clientes_final.csv
                     src/data/bronze/transacoes_final.csv
                           │
                           ▼
SILVER (Postgres/Supabase, SQL)
   ├─ silver_clientes            → clientes prontos p/ análise 
   └─ silver_transacoes          → transações válidas (só valores > 0)
                           │
                           ▼
GOLD (Postgres, SQL)
   └─ gold_clientes_rfv_produto  → cubo por cliente×produto com RFV e clusters


```

## O que cada etapa faz (BRONZE)

bronze_ingest.py
Lê os CSVs brutos, renomeia colunas, força tipos simples (datas, inteiros/decimais), remove espaços e acentos.

bronze_citymatch_clientes.py
Normaliza CIDADE com RapidFuzz: corrige variações (ex.: “mana”, “manaus”, “manao”) → “Manaus”; gera CIDADE_PADRONIZADA + score.

bronze_produtomatch_transacoes.py
Normaliza PRODUTO: mapeia variações (ex.: “vale”, “vale pre pago”, “recarg dig”) para
Vale Pre Pago ou Recarga Digital em PRODUTO_PADRONIZADO.

bronze_validate.py
Checagens de sanidade (nulos críticos, ids, faixas de data/valor) e salva:

        src/data/bronze/clientes_final.csv
        src/data/bronze/transacoes_final.csv

## Carregar os CSVs tratados no Postgres (SILVER)

Você pode usar Supabase (UI de import) ou psql. Abaixo, DDL + COPY via psql.

Criar tabelas Silver

```bash
-- Tabela de clientes
CREATE TABLE IF NOT EXISTS silver_clientes (
  "ID_PESSOA"           BIGINT PRIMARY KEY,
  "NIVEL_CLIENTE"       NUMERIC(6,0),
  "DATA_ALTERACAO_NIVEL" DATE,
  "CIDADE"              TEXT,
  "CIDADE_PADRONIZADA"  TEXT,
  "CITY_MATCH_KEY"      TEXT,
  "CITY_MATCH_SCORE"    NUMERIC,
  "INGEST_DATE"         DATE
);

-- Tabela de transações
CREATE TABLE IF NOT EXISTS silver_transacoes (
  "DATA"               DATE,
  "ID_PESSOA"          BIGINT,
  "NUM_LOJA"           TEXT,
  "PRODUTO"            TEXT,
  "PRODUTO_PADRONIZADO" TEXT,
  "VALOR_TRANSACAO"    NUMERIC(18,2),
  "INGEST_DATE"        DATE
);
```

Importar os CSVs gerados na Bronze

## GOLD: o que é o cubo e como o RFV funciona

Objeto: gold_clientes_rfv_produto (Materialized View).
Cada linha = cliente × produto com métricas e scores.

### Métricas calculadas

| Dimensão | O que mede | Interpretação de negócio | Quanto maior… |
|-----------|-------------|---------------------------|----------------|
| **Recência** | Dias desde a última transação | Atividade recente / risco de churn | **Melhor** (cliente está “quente”) |
| **Frequência** | Nº de transações no período | Hábito e recorrência | **Melhor** (uso mais constante) |
| **Valor** | Soma transacionada | Potencial de receita/LTV | **Melhor** (cliente vale mais) |


##3 Scores RFV (1–5)
Regras de pontuação (exemplo usado no case)

> As faixas são ajustáveis conforme o perfil do negócio; aqui priorizamos leitura executiva.

#### 🔹 R — Recência (dias)

| Recência (dias) | Score R |
|------------------:|:--------:|
| ≤ 30 | **5** |
| 31–60 | **4** |
| 61–120 | **3** |
| 121–240 | **2** |
| > 240 | **1** |

#### 🔹 F — Frequência (nº transações)

| Transações | Score F |
|-------------:|:--------:|
| ≥ 10 | **5** |
| 6–9 | **4** |
| 3–5 | **3** |
| 1–2 | **2** |
| 0 | **1** |

#### 🔹 V — Valor (R$ no período)

| Valor (R$) | Score V |
|--------------:|:--------:|
| ≥ 1.000 | **5** |
| 500–999 | **4** |
| 200–499 | **3** |
| 50–199 | **2** |
| < 50 | **1** |


### Do RFV ao **cluster final**

| **RFV (R+F+V)** | **Cluster sugerido** | **Direção de negócio** |
|:----------------:|:--------------------|:-----------------------|
| **13–15** | **Clientes VIP** | Fidelizar, upgrades, benefícios exclusivos |
| **10–12** | **Engajados** | Incentivar frequência e aumentar ticket |
| **7–9** | **Em risco** | Campanhas de reativação direcionadas |
| **4–6** | **Adormecidos** | Ofertas de retorno / cashback |
| **3** | **Inativos** | Reaquisição e campanhas de volta |

A view também calcula RFV por produto e RFV total do cliente, permitindo comparar o engajamento específico (ex.: Recarga Digital) vs. o geral.

### Como o RFV é aplicado na camada GOLD

Na camada **Gold**, a materialized view `gold_clientes_rfv_produto` contém:

- RFV **por produto** (ex.: Recarga Digital × Vale Pré-Pago)
- RFV **total do cliente** (consolidado de todos os produtos)
- **Clusters prontos** (`cluster_rfv_prod`, `cluster_rfv_total`) para segmentação.

**Métricas disponíveis:**
- `recencia_dias_prod`, `frequencia_prod`, `valor_total_prod`
- `recencia_dias_total`, `frequencia_total`, `valor_total_geral`
- `r_score_*`, `f_score_*`, `v_score_*`, `rfv_score_*`, `cluster_rfv_*`

 **Com isso, você pode:**
- Comparar **engajamento entre produtos**  
- Priorizar **reativação por valor e recência**  
