"""
Bronze Validado (pré-Silver)
- Lê os CSVs do bronze/landing (data mais recente)
- Aplica limpeza mínima e segura (sem regra de negócio)
- Escreve em bronze/validated mantendo a mesma data
"""

from pathlib import Path
import pandas as pd
import yaml
import re


def load_config(path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# Encontra a partição data=YYYY-MM-DD mais recente.
def latest_partition_path(root: Path) -> Path:
    parts = sorted([p for p in root.glob("data=*") if p.is_dir()], reverse=True)
    if not parts:
        raise FileNotFoundError(f"Nenhuma partição encontrada em {root}")
    return parts[0]

# UPPER_SNAKE_CASE simples, remove espaços e caracteres estranhos.
def normalize_colname(name: str) -> str:
    n = re.sub(r"\s+", "_", name.strip())
    n = re.sub(r"[^\w]", "_", n)  # só letras/números/underscore
    n = re.sub(r"_+", "_", n)
    return n.upper().strip("_")

# Tira espaços em colunas de texto.
def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        if pd.api.types.is_string_dtype(df[c]) or df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip()
    return df

# Remove linhas 100% vazias (tudo NaN ou strings vazias).
def drop_all_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    # converte strings vazias em NaN para facilitar o drop
    df = df.replace({"": pd.NA})
    return df.dropna(how="all")


# ------------- validação e tipos --------------

# Validação e tipagem da tabela clientes 
def validate_clientes(df: pd.DataFrame) -> pd.DataFrame:
    # Tipos básicos (sem regra de negócio)
    if "DATA_ALTERACAO_NIVEL" in df.columns:
        df["DATA_ALTERACAO_NIVEL"] = pd.to_datetime(df["DATA_ALTERACAO_NIVEL"], errors="coerce")
    # NIVEL_CLIENTE como numérico tolerante
    if "NIVEL_CLIENTE" in df.columns:
        df["NIVEL_CLIENTE"] = pd.to_numeric(df["NIVEL_CLIENTE"], errors="coerce")
    return df

# Validação e tipagem da tabela transações 
def validate_transacoes(df: pd.DataFrame) -> pd.DataFrame:
    if "DATA" in df.columns:
        df["DATA"] = pd.to_datetime(df["DATA"], errors="coerce")
    if "VALOR_TRANSACAO" in df.columns:
        df["VALOR_TRANSACAO"] = pd.to_numeric(df["VALOR_TRANSACAO"], errors="coerce")
    # NUM_LOJA como texto para preservar zeros à esquerda
    if "NUM_LOJA" in df.columns:
        df["NUM_LOJA"] = df["NUM_LOJA"].astype(str).str.strip()
    return df

# Processamento
def process_one(input_csv: Path, output_csv: Path, dataset: str, ingest_date: str):
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            df = pd.read_csv(input_csv, encoding=enc)
            break
        except Exception:
            continue

    # padroniza nomes de colunas
    df.columns = [normalize_colname(c) for c in df.columns]

    # limpeza mínima
    df = strip_strings(df)
    df = drop_all_empty_rows(df)

    # coerção de tipos básica por dataset
    if dataset == "clientes":
        df = validate_clientes(df)
    else:
        df = validate_transacoes(df)

    # coluna técnica
    df["INGEST_DATE"] = ingest_date

    # salva
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    print(f"[OK] {dataset} validado → {output_csv}")

def main():
    cfg = load_config()
    landing_root = Path(cfg["paths"]["bronze_landing"])
    validated_root = Path(cfg["paths"]["bronze_landing"]).parent / "validated"

    # pega a partição mais recente do landing
    latest_part = latest_partition_path(landing_root)
    ingest_date = latest_part.name.split("data=")[-1]

    # caminhos de entrada
    in_clientes = latest_part / "clientes" / cfg["files"]["clientes"]
    in_trans = latest_part / "transacoes" / cfg["files"]["transacoes"]

    # caminhos de saída mantendo a mesma data
    out_clientes = validated_root / f"data={ingest_date}" / "clientes" / "clientes_validated.csv"
    out_trans = validated_root / f"data={ingest_date}" / "transacoes" / "transacoes_validated.csv"

    process_one(in_clientes, out_clientes, "clientes", ingest_date)
    process_one(in_trans, out_trans, "transacoes", ingest_date)

    print("\n Bronze Validado concluído.")

if __name__ == "__main__":
    main()
