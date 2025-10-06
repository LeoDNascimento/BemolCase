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
import yaml
import pandas as pd


# Lê o arquivo de configuração (paths e nomes dos arquivos)
def load_config(path: str | Path = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# Garante que o diretório existe antes de salvar algo
def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

# Gera o hash SHA256 do arquivo (para controle de integridade)
def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

# Tenta ler o CSV com diferentes encodings comuns (UTF-8, Latin-1, CP1252)
def try_read_csv(path: Path, encodings=("utf-8", "latin-1", "cp1252")):
    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc)
            return df, enc
        except Exception:
            pass
    raise ValueError(f"Não foi possível ler o arquivo {path.name} com os encodings padrões.")


# Gera um resumo leve da base (linhas, colunas e percentual de nulos)
def profile_df(df: pd.DataFrame) -> dict:
    return {
        "n_rows": len(df),
        "n_cols": df.shape[1],
        "columns": list(df.columns),
        "nulls_pct_by_col": {c: round(df[c].isna().mean(), 4) for c in df.columns},
    }

# Realiza a ingestão de um único dataset (clientes ou transações)
def ingest_one(cfg: dict, dataset_name: str, file_key: str):
    # caminhos
    raw_path = Path(cfg["paths"]["raw_dir"]) / dataset_name
    bronze_root = Path(cfg["paths"]["bronze_landing"])
    catalog = Path(cfg["paths"]["catalog_dir"])
    ensure_dir(bronze_root)
    ensure_dir(catalog)

    # arquivo de origem
    src = raw_path / cfg["files"][file_key]
    if not src.exists():
        print(f"Arquivo não encontrado: {src}")
        return

    # data atual para organizar a pasta de saída
    now = datetime.utcnow()
    ds = now.strftime("%Y-%m-%d")

    # cria diretório de destino datado
    dst_dir = bronze_root / f"data={ds}" / dataset_name
    ensure_dir(dst_dir)
    dst = dst_dir / src.name

    # copia o arquivo bruto (sem alterações)
    dst.write_bytes(src.read_bytes())

    # tenta ler o arquivo para gerar o perfil
    df, enc = try_read_csv(src)

    # metadados principais
    meta = {
        "dataset": dataset_name,
        "source_file": str(src),
        "bronze_path": str(dst),
        "ingest_ts": now.isoformat() + "Z",
        "encoding": enc,
        "sha256": sha256_of_file(src),
        "profile": profile_df(df),
    }

    # salva metadados em JSON (ao lado do CSV)
    with (dst.with_suffix(".meta.json")).open("w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    # adiciona o mesmo registro no log geral
    with (Path(catalog) / "ingest_log.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(meta, ensure_ascii=False) + "\n")

    print(f"[OK] {dataset_name}: {meta['profile']['n_rows']} linhas, encoding={enc}")


# Função principal: executa a ingestão dos dois arquivos
def main():
    cfg = load_config()
    ingest_one(cfg, "clientes", "clientes")
    ingest_one(cfg, "transacoes", "transacoes")
    print("\n Etapa Bronze concluída.")


if __name__ == "__main__":
    main()