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
## 🏗️ Estrutura do Projeto

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
