from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime
from math import sqrt
import difflib

import pandas as pd


RAW_DIR = Path("raw")
OUT_DIR = Path("out")
CFG_DIR = Path("config")

PREFERRED_SHEET_NAME = "labview"

B_ETANOL_COL_CANDIDATES = ["B_Etanol", "B_ETANOL", "B_ETANOL (kg)", "B_Etanol (kg)"]

SAMPLES_PER_WINDOW = 30
MIN_SAMPLES_PER_WINDOW = 30
DT_S = 1.0  # dt = 1 s


@dataclass(frozen=True)
class FileMeta:
    path: Path
    basename: str
    source_type: str
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


def _normalize_cols(cols: List[str]) -> List[str]:
    return [str(c).replace("\ufeff", "").strip() for c in cols]


def list_sheet_names_xlsx(path: Path) -> List[str]:
    xf = pd.ExcelFile(path, engine="calamine")
    return list(xf.sheet_names)


def choose_labview_sheet(path: Path) -> str:
    sheets = list_sheet_names_xlsx(path)
    if not sheets:
        raise ValueError(f"Nenhuma aba encontrada em {path.name}")

    for s in sheets:
        if s.strip().lower() == PREFERRED_SHEET_NAME.lower():
            return s

    for s in sheets:
        if "labview" in s.strip().lower():
            return s

    if len(sheets) == 1:
        return sheets[0]

    raise ValueError(
        f"Não encontrei aba '{PREFERRED_SHEET_NAME}' e existem múltiplas abas em {path.name}: {sheets}. "
        f"Renomeie a aba no XLSX ou ajuste PREFERRED_SHEET_NAME."
    )


def find_b_etanol_col(df: pd.DataFrame) -> str:
    for c in B_ETANOL_COL_CANDIDATES:
        if c in df.columns:
            return c
    raise KeyError(
        f"Não encontrei coluna de balança. Procurei: {B_ETANOL_COL_CANDIDATES}. "
        f"Colunas disponíveis (primeiras 40): {list(df.columns)[:40]}"
    )


def resolve_col(df: pd.DataFrame, requested: str) -> str:
    """Resolve nome de coluna de forma case-insensitive e com sugestão."""
    if requested in df.columns:
        return requested
    # case-insensitive
    low_map = {c.lower(): c for c in df.columns}
    if requested.lower() in low_map:
        return low_map[requested.lower()]

    suggestion = difflib.get_close_matches(requested, list(df.columns), n=1)
    sug_txt = f" Sugestão: {suggestion[0]}" if suggestion else ""
    raise KeyError(f"Coluna '{requested}' não encontrada no dataframe.{sug_txt}")


# -------------------------
# Read LabVIEW
# -------------------------
def read_labview_xlsx(meta: FileMeta) -> pd.DataFrame:
    sheet = choose_labview_sheet(meta.path)
    df = pd.read_excel(meta.path, sheet_name=sheet, engine="calamine")
    df.columns = _normalize_cols(list(df.columns))
    df = df.loc[:, ~pd.Series(df.columns).astype(str).str.startswith("Unnamed").values].copy()

    df = df.reset_index(drop=True).copy()
    df["Index"] = range(len(df))
    df["WindowID"] = df["Index"] // SAMPLES_PER_WINDOW

    df.insert(0, "BaseName", meta.basename)
    df.insert(1, "Load_kW", meta.load_kw)
    df.insert(2, "EtOH_pct", meta.etoh_pct)
    df.insert(3, "H2O_pct", meta.h2o_pct)

    return df


# -------------------------
# Config Excel
# -------------------------
def load_config_excel() -> tuple[dict, dict, float]:
    p = CFG_DIR / "config_incertezas.xlsx"
    if not p.exists():
        raise FileNotFoundError(f"Não encontrei {p}. Crie config/config_incertezas.xlsx (aba Mappings e Instruments).")

    m = pd.read_excel(p, sheet_name="Mappings", engine="calamine")
    m.columns = _normalize_cols(list(m.columns))
    needed_m = {"key", "col_mean", "col_sd"}
    if not needed_m.issubset(set(m.columns)):
        raise KeyError(f"Aba Mappings precisa de colunas {needed_m}. Encontradas: {list(m.columns)}")

    mappings: dict = {}
    for _, row in m.iterrows():
        key = str(row["key"]).strip()
        mappings[key] = {
            "mean": str(row["col_mean"]).strip() if pd.notna(row["col_mean"]) else "",
            "sd": str(row["col_sd"]).strip() if pd.notna(row["col_sd"]) else "",
        }

    ins = pd.read_excel(p, sheet_name="Instruments", engine="calamine")
    ins.columns = _normalize_cols(list(ins.columns))
    needed_i = {"key", "model", "dist", "percent", "digits", "lsd", "abs", "resolution"}
    if not needed_i.issubset(set(ins.columns)):
        raise KeyError(f"Aba Instruments precisa de colunas {needed_i}. Encontradas: {list(ins.columns)}")

    instruments: dict = {}
    for _, row in ins.iterrows():
        key = str(row["key"]).strip()
        instruments[key] = {
            "model": str(row["model"]).strip(),
            "dist": str(row["dist"]).strip().lower(),
            "percent": float(row["percent"] or 0.0),
            "digits": float(row["digits"] or 0.0),
            "lsd": float(row["lsd"] or 0.0),
            "abs": float(row["abs"] or 0.0),
            "resolution": float(row["resolution"] or 0.0),
        }

    k = 2.0
    return mappings, instruments, k


# -------------------------
# Type B helpers
# -------------------------
def rect_to_std(limit: pd.Series | float) -> pd.Series:
    return pd.to_numeric(limit, errors="coerce") / sqrt(3)


def res_to_std(step: float) -> float:
    return step / sqrt(12) if step > 0 else 0.0


def uB_power_kw(P: pd.Series, spec: dict) -> pd.Series:
    P = pd.to_numeric(P, errors="coerce")
    limit = abs(spec.get("percent", 0.0)) * P.abs() + abs(spec.get("digits", 0.0)) * abs(spec.get("lsd", 0.0)) + abs(spec.get("abs", 0.0))
    if spec.get("dist", "rect") == "rect":
        return rect_to_std(limit)
    return limit


def uB_direct(value: pd.Series, spec: dict) -> pd.Series:
    v = pd.to_numeric(value, errors="coerce")
    limit = abs(spec.get("abs", 0.0)) + abs(spec.get("percent", 0.0)) * v.abs()
    u_acc = rect_to_std(limit) if spec.get("dist", "rect") == "rect" else limit
    u_res = res_to_std(abs(spec.get("resolution", 0.0)))
    return (u_acc**2 + u_res**2) ** 0.5


# -------------------------
# Trechos stats
# -------------------------
def compute_trechos_stats(lv_raw: pd.DataFrame, instruments: dict) -> pd.DataFrame:
    bcol = find_b_etanol_col(lv_raw)

    group_cols = ["BaseName", "Load_kW", "EtOH_pct", "H2O_pct", "WindowID"]
    ignore_cols = set(group_cols + ["Index"])
    candidate_cols = [c for c in lv_raw.columns if c not in ignore_cols]

    lv = lv_raw.copy()
    for c in candidate_cols:
        lv[c] = pd.to_numeric(lv[c], errors="coerce")

    g = lv.groupby(group_cols, dropna=False, sort=True)

    n = g.size().rename("N_samples")
    valid_idx = n[n >= MIN_SAMPLES_PER_WINDOW].index
    if len(valid_idx) == 0:
        return pd.DataFrame(columns=group_cols + ["N_samples", "Consumo_kg_h"])

    lv_valid = lv.set_index(group_cols).loc[valid_idx].reset_index()
    gv = lv_valid.groupby(group_cols, dropna=False, sort=True)

    means = gv[candidate_cols].mean(numeric_only=True).add_suffix("_mean")
    sds = gv[candidate_cols].std(ddof=1, numeric_only=True).add_suffix("_sd")

    first = gv[bcol].first().rename("BEtanol_start")
    last = gv[bcol].last().rename("BEtanol_end")
    n2 = gv.size().rename("N_samples")

    out = pd.concat([means, sds, first, last, n2], axis=1).reset_index()

    out["Delta_BEtanol"] = out["BEtanol_start"] - out["BEtanol_end"]
    out["DeltaT_s"] = (out["N_samples"] - 1) * DT_S
    out["Consumo_kg_h"] = (out["Delta_BEtanol"] / out["DeltaT_s"]) * 3600.0
    out.loc[out["DeltaT_s"] <= 0, "Consumo_kg_h"] = pd.NA

    bal = instruments.get("balance_kg", {"resolution": 0.001, "dist": "rect"})
    res_kg = float(bal.get("resolution", 0.001) or 0.001)
    u_read = res_to_std(res_kg)          # kg
    u_delta = sqrt(2) * u_read           # kg
    out["uB_Consumo_kg_h"] = (u_delta / out["DeltaT_s"]) * 3600.0
    out.loc[out["DeltaT_s"] <= 0, "uB_Consumo_kg_h"] = pd.NA

    return out.copy()


def compute_ponto_stats(trechos: pd.DataFrame) -> pd.DataFrame:
    if trechos.empty:
        return pd.DataFrame()

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

    n_trechos = g.size().rename("N_trechos_validos")

    out = pd.concat([mean_of_windows, sd_of_windows, delta_df, n_trechos], axis=1).reset_index()
    return out.copy()


# -------------------------
# LHV lookup
# -------------------------
def load_lhv_lookup() -> pd.DataFrame:
    p = CFG_DIR / "lhv.csv"
    if not p.exists():
        raise FileNotFoundError(f"Não encontrei {p}. Crie config/lhv.csv")

    df = pd.read_csv(p, sep=None, engine="python", encoding="utf-8-sig")
    df.columns = _normalize_cols(list(df.columns))

    colmap: Dict[str, str] = {}
    for c in df.columns:
        cl = c.lower()
        if cl in {"etoh_pct", "etoh", "e_pct", "e"}:
            colmap[c] = "EtOH_pct"
        elif cl in {"h2o_pct", "h2o", "h20_pct", "h20", "h_pct", "h"}:
            colmap[c] = "H2O_pct"
        elif cl in {"lhv_kj_kg", "lhv", "pci_kj_kg", "pci"}:
            colmap[c] = "LHV_kJ_kg"

    df = df.rename(columns=colmap)

    needed = {"EtOH_pct", "H2O_pct", "LHV_kJ_kg"}
    missing = needed - set(df.columns)
    if missing:
        raise KeyError(f"lhv.csv sem colunas {missing}. Colunas encontradas: {list(df.columns)}")

    df["EtOH_pct"] = pd.to_numeric(df["EtOH_pct"], errors="coerce").astype("Int64")
    df["H2O_pct"] = pd.to_numeric(df["H2O_pct"], errors="coerce").astype("Int64")
    df["LHV_kJ_kg"] = pd.to_numeric(df["LHV_kJ_kg"], errors="coerce")

    return df


# -------------------------
# KPI + uncertainty
# -------------------------
def compute_kpis(ponto: pd.DataFrame, lhv: pd.DataFrame, mappings: dict, instruments: dict, k: float) -> pd.DataFrame:
    if ponto.empty:
        return pd.DataFrame()

    df = ponto.merge(lhv, on=["EtOH_pct", "H2O_pct"], how="left")

    P_mean_req = mappings["power_kw"]["mean"]
    P_sd_req = mappings["power_kw"]["sd"]
    F_mean_req = mappings["fuel_kgh"]["mean"]
    F_sd_req = mappings["fuel_kgh"]["sd"]
    L_req = mappings["lhv_kj_kg"]["mean"]

    P_mean = resolve_col(df, P_mean_req)
    F_mean = resolve_col(df, F_mean_req)
    L_col = resolve_col(df, L_req)

    P_sd = resolve_col(df, P_sd_req) if P_sd_req else ""
    F_sd = resolve_col(df, F_sd_req) if F_sd_req else ""

    N = pd.to_numeric(df.get("N_trechos_validos", pd.NA), errors="coerce")

    uA_P = pd.to_numeric(df[P_sd], errors="coerce") / (N**0.5) if P_sd and P_sd in df.columns else pd.Series(pd.NA, index=df.index)
    uA_F = pd.to_numeric(df[F_sd], errors="coerce") / (N**0.5) if F_sd and F_sd in df.columns else pd.Series(pd.NA, index=df.index)

    uB_P = uB_power_kw(df[P_mean], instruments.get("power_kw", {}))

    uB_F_col = "uB_Consumo_kg_h_mean_of_windows"
    if uB_F_col in df.columns:
        uB_F = pd.to_numeric(df[uB_F_col], errors="coerce")
    else:
        uB_F = uB_direct(df[F_mean], instruments.get("fuel_kgh", {}))

    uB_L = uB_direct(df[L_col], instruments.get("lhv_kj_kg", {}))

    df["Consumo_kg_h_pm"] = pd.to_numeric(df[F_sd], errors="coerce") if F_sd and F_sd in df.columns else pd.NA
    df["Consumo_kg_h_low"] = pd.to_numeric(df[F_mean], errors="coerce") - df["Consumo_kg_h_pm"]
    df["Consumo_kg_h_high"] = pd.to_numeric(df[F_mean], errors="coerce") + df["Consumo_kg_h_pm"]

    mdot = pd.to_numeric(df[F_mean], errors="coerce") / 3600.0
    PkW = pd.to_numeric(df[P_mean], errors="coerce")
    LHV = pd.to_numeric(df[L_col], errors="coerce")

    df["n_th"] = PkW / (mdot * LHV)
    df.loc[(mdot <= 0) | (LHV <= 0) | (PkW <= 0), "n_th"] = pd.NA
    df["n_th_pct"] = df["n_th"] * 100.0

    Fkgh = pd.to_numeric(df[F_mean], errors="coerce")

    relA = ((uA_P / PkW) ** 2 + (uA_F / Fkgh) ** 2) ** 0.5
    relB = ((uB_P / PkW) ** 2 + (uB_F / Fkgh) ** 2 + (uB_L / LHV) ** 2) ** 0.5

    df["uA_n_th"] = df["n_th"] * relA
    df["uB_n_th"] = df["n_th"] * relB
    df["uc_n_th"] = (df["uA_n_th"] ** 2 + df["uB_n_th"] ** 2) ** 0.5
    df["U_n_th"] = k * df["uc_n_th"]

    df["uA_n_th_pct"] = df["uA_n_th"] * 100.0
    df["uB_n_th_pct"] = df["uB_n_th"] * 100.0
    df["U_n_th_pct"] = df["U_n_th"] * 100.0

    # Reorder helpful block
    key_cols = ["BaseName", "Load_kW", "EtOH_pct", "H2O_pct", "N_trechos_validos"]
    pref = key_cols + [P_mean] + ([P_sd] if P_sd else []) + [F_mean] + ([F_sd] if F_sd else []) + ["Consumo_kg_h_pm","Consumo_kg_h_low","Consumo_kg_h_high", L_col, "n_th","n_th_pct","uA_n_th","uB_n_th","uc_n_th","U_n_th","U_n_th_pct"]
    pref = [c for c in pref if c in df.columns]
    rest = [c for c in df.columns if c not in pref]
    return df[pref + rest].copy()


def safe_to_excel(df: pd.DataFrame, path: Path) -> Path:
    try:
        df.to_excel(path, index=False)
        return path
    except PermissionError:
        ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        alt = path.with_name(f"{path.stem}_{ts}{path.suffix}")
        df.to_excel(alt, index=False)
        return alt


def main() -> None:
    OUT_DIR.mkdir(exist_ok=True)

    raw_files = [p for p in RAW_DIR.glob("*") if p.is_file() and not p.name.startswith("~$")]

    metas = [parse_meta(p) for p in raw_files]
    lv_files = [m for m in metas if m.source_type == "LABVIEW" and m.path.suffix.lower() == ".xlsx"]
    if not lv_files:
        raise SystemExit("Não achei .xlsx do LabVIEW em raw/ (ou só existem arquivos ~$/temporários).")

    mappings, instruments, k = load_config_excel()

    lv_all: List[pd.DataFrame] = []
    for m in lv_files:
        try:
            df_i = read_labview_xlsx(m)
            if not df_i.empty:
                lv_all.append(df_i)
        except Exception as e:
            print(f"[ERROR] Falha lendo {m.path.name}: {e}")

    if not lv_all:
        raise SystemExit("Nenhum arquivo LabVIEW foi lido com sucesso.")

    lv_raw = pd.concat(lv_all, ignore_index=True)

    trechos = compute_trechos_stats(lv_raw, instruments)
    ponto = compute_ponto_stats(trechos)

    lhv = load_lhv_lookup()
    kpis = compute_kpis(ponto, lhv, mappings, instruments, k)

    lv_raw.to_parquet(OUT_DIR / "lv_raw.parquet", index=False)
    trechos.to_parquet(OUT_DIR / "lv_trechos.parquet", index=False)
    ponto.to_parquet(OUT_DIR / "lv_ponto.parquet", index=False)

    out_xlsx = safe_to_excel(kpis, OUT_DIR / "lv_kpis.xlsx")

    print("OK! Gerado:")
    print(" - out/lv_raw.parquet")
    print(" - out/lv_trechos.parquet")
    print(" - out/lv_ponto.parquet")
    print(f" - {out_xlsx} (n_th + incertezas A/B/total)")


if __name__ == "__main__":
    main()