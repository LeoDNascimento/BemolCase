"""
Camada Bronze (Ingestão)
- Congela arquivos brutos em /src/data/bronze/landing/<data>/
- Gera metadados (sha256, encoding usado, contagem de linhas/colunas)
- Salva log de ingestão em /src/catalog/ingest_log.jsonl
"""


import hashlib
import json
from datetime import datetime
from pathlib import Path

import pandas as pd