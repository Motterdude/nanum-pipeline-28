from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List

import pandas as pd


RAW_DIR = Path("raw")
OUT_DIR = Path("out")
CFG_DIR = Path("config")

# Se existir, usamos; se não existir, o script tenta achar automaticamente
PREFERRED_SHEET_NAME = "labview"

# Possíveis nomes de coluna da balança
B_ETANOL_COL_CANDIDATES = ["B_Etanol", "B_ETANOL", "B_ETANOL (kg)", "B_Etanol (kg)"]


@dataclass(frozen=True)
class FileMeta:
    path: Path
    basename: str
    source_type: str  # "LABVIEW" or "KIBOX"
    load_kw: Optional[int]
    etoh_pct: Optional[int]
    h2o_pct: Optional[int]


def parse_meta(path: Path) -> FileMeta:
    basename = path.stem

    source_type = "KIBOX" if basename.lower().endswith("_i") else "LABVIEW"

    m_kw = re.search(r"(\d+)\s*kw", basename, flags=re.IGNORECASE)
    load_kw = int(m_kw.group(1)) if m_kw else None

    m_eh = re.search(r"E(\d+)\s*H(\d+)", basename, flags=re.IGNORECASE)
    etoh_pct = int(m_eh.group(1)) if m_eh else None
    h2o_pct = int(m_eh.group(2)) if m_eh else None

    return FileMeta(
        path=path,
        basename=basename,
        source_type=source_type,
        load_kw=load_kw,
        etoh_pct=etoh_pct,
        h2o_pct=h2o_pct,
    )


def find_b_etanol_col(df: pd.DataFrame) -> str:
    for c in B_ETANOL_COL_CANDIDATES:
        if c in df.columns:
            return c
    raise KeyError(
        f"Não encontrei coluna de balança. Procurei: {B_ETANOL_COL_CANDIDATES}. "
        f"Colunas disponíveis (primeiras 40): {list(df.columns)[:40]}"
    )


def take_mid_240(df: pd.DataFrame) -> pd.DataFrame:
    """Mantém 240 amostras do miolo. Se tiver menos, completa repetindo a última linha.
    Depois cria Index240 (0..239) e WindowID (0..7)."""
    n = len(df)
    if n == 0:
        out = df.copy()
        out["Index240"] = []
        out["WindowID"] = []
        return out

    if n >= 240:
        start = (n - 240) // 2
        out = df.iloc[start : start + 240].copy()
    else:
        pad_n = 240 - n
        out = df.copy()
        last = out.iloc[[-1]].copy()
        out = pd.concat([out, pd.concat([last] * pad_n, ignore_index=True)], ignore_index=True)

    out.reset_index(drop=True, inplace=True)
    out["Index240"] = range(240)
    out["WindowID"] = out["Index240"] // 30  # 0..7 garantido
    return out


def list_sheet_names_xlsx(path: Path) -> List[str]:
    """Lista abas usando engine=calamine."""
    xf = pd.ExcelFile(path, engine="calamine")
    return list(xf.sheet_names)


def choose_labview_sheet(path: Path) -> str:
    """Escolhe a aba do LabVIEW:
    1) tenta PREFERRED_SHEET_NAME
    2) tenta achar uma aba que contenha 'labview'
    3) se só tiver uma aba, usa ela
    4) senão, erro explícito
    """
    sheets = list_sheet_names_xlsx(path)
    if not sheets:
        raise ValueError(f"Nenhuma aba encontrada em {path.name}")

    # 1) Preferida
    for s in sheets:
        if s.strip().lower() == PREFERRED_SHEET_NAME.lower():
            return s

    # 2) Contém 'labview'
    for s in sheets:
        if "labview" in s.strip().lower():
            return s

    # 3) Só uma aba
    if len(sheets) == 1:
        return sheets[0]

    raise ValueError(
        f"Não encontrei aba '{PREFERRED_SHEET_NAME}' e existem múltiplas abas em {path.name}: {sheets}. "
        f"Renomeie a aba no XLSX ou ajuste PREFERRED_SHEET_NAME."
    )


def read_labview_xlsx(meta: FileMeta) -> pd.DataFrame:
    """Lê XLSX do LabVIEW usando SOMENTE calamine (robusto a estilos)."""
    sheet = choose_labview_sheet(meta.path)

    df = pd.read_excel(meta.path, sheet_name=sheet, engine="calamine")

    # remove colunas "Unnamed"
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")].copy()

    df240 = take_mid_240(df)

    # metadados
    df240.insert(0, "BaseName", meta.basename)
    df240.insert(1, "Load_kW", meta.load_kw)
    df240.insert(2, "EtOH_pct", meta.etoh_pct)
    df240.insert(3, "H2O_pct", meta.h2o_pct)

    return df240


def compute_trechos_stats(lv240: pd.DataFrame) -> pd.DataFrame:
    bcol = find_b_etanol_col(lv240)

    group_cols = ["BaseName", "Load_kW", "EtOH_pct", "H2O_pct", "WindowID"]
    num_cols = [c for c in lv240.columns if c not in group_cols and c != "Index240"]

    lv_num = lv240.copy()
    for c in num_cols:
        lv_num[c] = pd.to_numeric(lv_num[c], errors="coerce")

    g = lv_num.groupby(group_cols, dropna=False, sort=True)

    means = g[num_cols].mean(numeric_only=True).add_suffix("_mean")
    sds = g[num_cols].std(ddof=1, numeric_only=True).add_suffix("_sd")

    first = g[bcol].first().rename("BEtanol_start")
    last = g[bcol].last().rename("BEtanol_end")
    n = g.size().rename("N_samples")

    out = pd.concat([means, sds, first, last, n], axis=1).reset_index()

    out["Delta_BEtanol"] = out["BEtanol_start"] - out["BEtanol_end"]
    out["DeltaT_s"] = (out["N_samples"] - 1) * 1.0
    out["Consumo_kg_h"] = (out["Delta_BEtanol"] / out["DeltaT_s"]) * 3600.0
    out.loc[out["DeltaT_s"] <= 0, "Consumo_kg_h"] = pd.NA

    return out


def compute_ponto_stats(trechos: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["BaseName", "Load_kW", "EtOH_pct", "H2O_pct"]
    value_cols = [c for c in trechos.columns if c not in group_cols and c != "WindowID"]

    tre = trechos.copy()
    for c in value_cols:
        tre[c] = pd.to_numeric(tre[c], errors="coerce")

    g = tre.groupby(group_cols, dropna=False, sort=True)

    mean_of_windows = g[value_cols].mean(numeric_only=True).add_suffix("_mean_of_windows")
    sd_of_windows = g[value_cols].std(ddof=1, numeric_only=True).add_suffix("_sd_of_windows")

    tre_sorted = tre.sort_values(group_cols + ["WindowID"])
    g2 = tre_sorted.groupby(group_cols, dropna=False, sort=True)
    first = g2[value_cols].first().add_suffix("_first")
    last = g2[value_cols].last().add_suffix("_last")
    delta = (last.values - first.values)

    delta_df = pd.DataFrame(
        delta,
        index=first.index,
        columns=[c.replace("_first", "_delta_first_last") for c in first.columns],
    )

    n_trechos = g.size().rename("N_trechos")

    out = pd.concat([mean_of_windows, sd_of_windows, delta_df, n_trechos], axis=1).reset_index()
    return out


def load_lhv_lookup() -> pd.DataFrame:
    p = CFG_DIR / "lhv.csv"
    df = pd.read_csv(p)
    df["EtOH_pct"] = pd.to_numeric(df["EtOH_pct"], errors="coerce").astype("Int64")
    df["H2O_pct"] = pd.to_numeric(df["H2O_pct"], errors="coerce").astype("Int64")
    df["LHV_kJ_kg"] = pd.to_numeric(df["LHV_kJ_kg"], errors="coerce")
    return df


def compute_kpis(ponto: pd.DataFrame, lhv: pd.DataFrame) -> pd.DataFrame:
    df = ponto.merge(lhv, on=["EtOH_pct", "H2O_pct"], how="left")

    cons_col = "Consumo_kg_h_mean_of_windows"
    pow_col = "Potência Total_mean_mean_of_windows"

    if cons_col not in df.columns:
        raise KeyError(f"Não achei coluna {cons_col}")
    if pow_col not in df.columns:
        raise KeyError(
            f"Não achei coluna {pow_col}. "
            "A potência pode ter outro nome no seu LabVIEW; me diga o nome da coluna que aparece no out/lv_trechos.parquet."
        )

    df["mdot_f_kg_s"] = df[cons_col] / 3600.0
    df["Eta_th"] = df[pow_col] / (df["mdot_f_kg_s"] * df["LHV_kJ_kg"])
    df.loc[(df["mdot_f_kg_s"] <= 0) | (df["LHV_kJ_kg"] <= 0), "Eta_th"] = pd.NA
    df["Eta_th_pct"] = df["Eta_th"] * 100.0
    return df


def main() -> None:
    OUT_DIR.mkdir(exist_ok=True)

    files = [p for p in RAW_DIR.glob("*") if p.is_file()]
    metas = [parse_meta(p) for p in files]

    lv_files: List[FileMeta] = [
        m for m in metas if m.source_type == "LABVIEW" and m.path.suffix.lower() == ".xlsx"
    ]
    if not lv_files:
        raise SystemExit("Não achei .xlsx do LabVIEW em raw/")

    lv240_all: List[pd.DataFrame] = []
    for m in lv_files:
        try:
            df_i = read_labview_xlsx(m)
            if not df_i.empty:
                lv240_all.append(df_i)
        except Exception as e:
            print(f"[ERROR] Falha lendo {m.path.name}: {e}")

    if not lv240_all:
        raise SystemExit(
            "Nenhum arquivo LabVIEW foi lido com sucesso. "
            "Verifique a aba do XLSX e/ou se os arquivos estão íntegros."
        )

    lv240 = pd.concat(lv240_all, ignore_index=True)

    trechos = compute_trechos_stats(lv240)
    ponto = compute_ponto_stats(trechos)

    lhv = load_lhv_lookup()
    kpis = compute_kpis(ponto, lhv)

    lv240.to_parquet(OUT_DIR / "lv_240.parquet", index=False)
    trechos.to_parquet(OUT_DIR / "lv_trechos.parquet", index=False)
    ponto.to_parquet(OUT_DIR / "lv_ponto.parquet", index=False)
    kpis.to_excel(OUT_DIR / "lv_kpis.xlsx", index=False)

    print("OK! Gerado:")
    print(" - out/lv_240.parquet")
    print(" - out/lv_trechos.parquet")
    print(" - out/lv_ponto.parquet")
    print(" - out/lv_kpis.xlsx")


if __name__ == "__main__":
    main()