"""
Pré-Silver: padronização de PRODUTO em transações.
Mantém apenas duas categorias canônicas: 'Vale Pre Pago' e 'Recarga Digital'.
"""

import pandas as pd
from pathlib import Path
import yaml
import unicodedata, re
from rapidfuzz import process, fuzz

def load_config(path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# Remove acentos, deixa minúsculo, tira símbolos e espaços extras.
def normalize(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = re.sub(r"[^a-z\s]", " ", s)
    return re.sub(r"\s{2,}", " ", s).strip()

def main():
    cfg = load_config()
    validated_root = Path(cfg["paths"]["bronze_landing"]).parent / "validated"
    latest = sorted(validated_root.glob("data=*"))[-1]

    in_trans = latest / "transacoes" / "transacoes_validated.csv"
    out_trans = latest / "transacoes" / "transacoes_final.csv"

    df = pd.read_csv(in_trans)
    if "PRODUTO" not in df.columns:
        raise ValueError("Coluna 'PRODUTO' não encontrada em transacoes_validated.csv")

    # canônicas
    canon = ["Vale Pre Pago", "Recarga Digital"]
    canon_norm = {normalize(c): c for c in canon}
    canon_keys = list(canon_norm.keys())

    def map_produto(x, threshold=80):
        q = normalize(x)
        if not q:
            return x
        best = process.extractOne(q, canon_keys, scorer=fuzz.WRatio)
        if best and best[1] >= threshold:
            return canon_norm[best[0]]
        return x  # mantém como está se não bater limiar

    df["PRODUTO_PADRONIZADO"] = df["PRODUTO"].map(map_produto)
    df.to_csv(out_trans, index=False)

    print(f"[OK] Produtos padronizados → {out_trans}")
    print("Valores únicos finais:", sorted(df["PRODUTO_PADRONIZADO"].unique()))

if __name__ == "__main__":
    main()
