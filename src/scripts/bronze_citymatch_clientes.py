"""
Pré-Silver: Padronização de CIDADE com fuzzy (rapidfuzz) para a tabela de clientes.
- Lê o bronze/validated mais recente
- Gera CIDADE_NORMALIZADA (chave) e CIDADE_PADRONIZADA (exibição)
- Usa matching aproximado para juntar variações parecidas
- Salva a tabela final e o mapa de cidades
"""
from pathlib import Path
import re
import unicodedata
import pandas as pd
import yaml
from rapidfuzz import process, fuzz


# ---------------- util ----------------
def load_config(path="config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def latest_partition(root: Path) -> Path:
    parts = sorted([p for p in root.glob("data=*") if p.is_dir()])
    if not parts:
        raise FileNotFoundError(f"Nenhuma partição encontrada em {root}")
    return parts[-1]

def read_csv_tolerant(path: Path) -> pd.DataFrame:
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    raise ValueError(f"Falha ao ler {path.name}")

# Remove acentos, baixa, tira símbolos e colapsa espaços.
def clean(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = re.sub(r"[^a-z\s]", " ", s)
    return re.sub(r"\s{2,}", " ", s).strip()

def titlecase(s: str) -> str:
    return " ".join(w.capitalize() for w in str(s).split())


# ---------------- principal ----------------
def main():
    cfg = load_config()

    # caminhos
    validated_root = Path(cfg["paths"]["bronze_landing"]).parent / "validated"
    part = latest_partition(validated_root)
    in_clientes = part / "clientes" / "clientes_validated.csv"
    out_clientes = part / "clientes" / "clientes_final.csv"
    out_auditoria = part / "clientes" / "city_mapping_audit.csv"
    out_clientes.parent.mkdir(parents=True, exist_ok=True)

    # ler clientes
    df = read_csv_tolerant(in_clientes)
    if "CIDADE" not in df.columns:
        raise ValueError("Coluna 'CIDADE' não encontrada em clientes_validated.csv")

    # lista canônica: tenta ler de arquivo; se não existir, infere do próprio dado
    # Só está funcionando com a dim, os dados errados são uma amostra muito grande
    canon_file = Path("src/dim/canon_cidades.txt")  # 1 cidade por linha (ex.: Manaus)
    if canon_file.exists():
        canon = [line.strip() for line in canon_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    else:
        # fallback: inferir top 5 formas mais frequentes (pelo texto original Title Case)
        top = (
            df["CIDADE"]
            .dropna()
            .astype(str)
            .str.strip()
            .str.title()
            .value_counts()
            .head(5)
            .index.tolist()
        )
        canon = top

    # preparar estruturas para fuzzy (chaves limpas)
    canon_clean = {clean(c): c for c in canon if clean(c)}
    canon_keys = list(canon_clean.keys())

    # função de mapeamento
    def map_city(x: str, threshold: int = 80) -> tuple[str, str, float]:
        """
        Retorna (canonical, matched_key, score)
        - canonical: cidade final exibível
        - matched_key: chave canônica limpa que bateu
        - score: pontuação do match
        """
        q = clean(x)
        if not q or not canon_keys:
            return x, "", 0.0
        best = process.extractOne(q, canon_keys, scorer=fuzz.WRatio)
        if best and best[1] >= threshold:
            return canon_clean[best[0]], best[0], float(best[1])
        return x, "", float(best[1] if best else 0.0)  # mantém original para revisão

    # aplicar
    mapped = df["CIDADE"].astype(str).apply(map_city)
    df["CIDADE_PADRONIZADA"] = mapped.map(lambda t: titlecase(t[0]))
    df["CITY_MATCH_KEY"] = mapped.map(lambda t: t[1])
    df["CITY_MATCH_SCORE"] = mapped.map(lambda t: t[2])

    # salvar tabela final e auditoria
    df.to_csv(out_clientes, index=False)

    audit = (
        df[["CIDADE", "CIDADE_PADRONIZADA", "CITY_MATCH_KEY", "CITY_MATCH_SCORE"]]
        .drop_duplicates()
        .sort_values(["CIDADE_PADRONIZADA", "CIDADE"])
    )
    audit.to_csv(out_auditoria, index=False)

    # feedback rápido
    ok = (df["CITY_MATCH_SCORE"] >= 80).mean() * 100
    print(f"[OK] clientes_final.csv → {out_clientes}")
    print(f"[OK] city_mapping_audit.csv → {out_auditoria}")
    print(f"Cobertura de match >= 80: {ok:.1f}%")

if __name__ == "__main__":
    main()