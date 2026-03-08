from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Tuple
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

K_COVERAGE = 2.0  # k=2 (pode virar config depois se quiser)


# -------------------------
# engine helper (calamine -> openpyxl fallback)
# -------------------------
def _excel_engine_preferred() -> str:
    """
    Returns best available excel engine.
    Prefers calamine when it is installed and pandas accepts it.
    Falls back to openpyxl otherwise.
    """
    # If python_calamine is installed, try using calamine
    try:
        import python_calamine  # noqa: F401
        # quick capability check: some environments have the lib but pandas doesn't register engine
        # We'll still try calamine first in the call sites, but this allows consistent selection.
        return "calamine"
    except Exception:
        return "openpyxl"


def _read_excel(path: Path, sheet_name: str | int | None = 0) -> pd.DataFrame:
    """
    Robust read_excel: tries calamine first (if present), then openpyxl.
    """
    eng = _excel_engine_preferred()
    if eng == "calamine":
        try:
            return pd.read_excel(path, sheet_name=sheet_name, engine="calamine")
        except Exception as e:
            print(f"[WARN] read_excel com calamine falhou em {path.name} (sheet={sheet_name}): {e}. Tentando openpyxl...")
    # fallback
    return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")


def _excel_file(path: Path) -> pd.ExcelFile:
    """
    Robust ExcelFile: tries calamine first (if present), then openpyxl.
    """
    eng = _excel_engine_preferred()
    if eng == "calamine":
        try:
            return pd.ExcelFile(path, engine="calamine")
        except Exception as e:
            print(f"[WARN] ExcelFile com calamine falhou em {path.name}: {e}. Tentando openpyxl...")
    return pd.ExcelFile(path, engine="openpyxl")


# -------------------------
# helpers
# -------------------------
def norm_key(x: object) -> str:
    return str(x).replace("\ufeff", "").strip().lower()


def _normalize_cols(cols: List[str]) -> List[str]:
    return [str(c).replace("\ufeff", "").strip() for c in cols]


def resolve_col(df: pd.DataFrame, requested: str) -> str:
    requested = str(requested).replace("\ufeff", "").strip()
    if not requested:
        raise KeyError("Nome de coluna solicitado está vazio (verifique Mappings no config_incertezas.xlsx).")

    if requested in df.columns:
        return requested

    low_map = {str(c).lower().strip(): c for c in df.columns}
    req_low = requested.lower().strip()
    if req_low in low_map:
        return low_map[req_low]

    suggestion = difflib.get_close_matches(requested, list(df.columns), n=5)
    sug_txt = f" Sugestões: {suggestion}" if suggestion else ""
    raise KeyError(f"Coluna '{requested}' não encontrada no dataframe.{sug_txt}")


def safe_to_excel(df: pd.DataFrame, path: Path) -> Path:
    try:
        df.to_excel(path, index=False)
        return path
    except PermissionError:
        ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        alt = path.with_name(f"{path.stem}_{ts}{path.suffix}")
        df.to_excel(alt, index=False)
        return alt


def rect_to_std(limit: pd.Series | float) -> pd.Series:
    return pd.to_numeric(limit, errors="coerce") / sqrt(3)


def res_to_std(step: float) -> float:
    # quantização: ±step/2 retangular => std = step/sqrt(12)
    return step / sqrt(12) if step > 0 else 0.0


def _to_float(x: object, default: float = 0.0) -> float:
    """
    Robust numeric parsing for Excel cells:
    - accepts None/NaN/"" -> default
    - accepts "0,25" -> 0.25
    - accepts "  1.2 " -> 1.2
    """
    if x is None:
        return default
    try:
        if pd.isna(x):
            return default
    except Exception:
        pass

    if isinstance(x, (int, float)):
        try:
            return float(x)
        except Exception:
            return default

    s = str(x).strip()
    if s == "":
        return default
    s = s.replace("\ufeff", "").strip()
    # decimal comma support
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return default


# -------------------------
# meta
# -------------------------
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


# -------------------------
# xlsx read
# -------------------------
def list_sheet_names_xlsx(path: Path) -> List[str]:
    xf = _excel_file(path)
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

    raise ValueError(f"Não encontrei aba '{PREFERRED_SHEET_NAME}' e existem múltiplas abas em {path.name}: {sheets}.")


def find_b_etanol_col(df: pd.DataFrame) -> str:
    for c in B_ETANOL_COL_CANDIDATES:
        if c in df.columns:
            return c
    raise KeyError(
        f"Não encontrei coluna de balança. Procurei: {B_ETANOL_COL_CANDIDATES}. "
        f"Colunas (primeiras 40): {list(df.columns)[:40]}"
    )


def read_labview_xlsx(meta: FileMeta) -> pd.DataFrame:
    sheet = choose_labview_sheet(meta.path)
    df = _read_excel(meta.path, sheet_name=sheet)

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
# config excel
# -------------------------
def load_config_excel() -> Tuple[dict, dict]:
    """
    Lê config/config_incertezas.xlsx
    Aba Mappings: key, col_mean, col_sd
    Aba Instruments: key, model, dist, percent, digits, lsd, abs, resolution
    """
    p = CFG_DIR / "config_incertezas.xlsx"
    if not p.exists():
        raise FileNotFoundError(f"Não encontrei {p}.")

    m = _read_excel(p, sheet_name="Mappings")
    m.columns = _normalize_cols(list(m.columns))
    need_m = {"key", "col_mean", "col_sd"}
    if not need_m.issubset(set(m.columns)):
        raise KeyError(f"Aba Mappings precisa de {need_m}. Encontradas: {list(m.columns)}")

    mappings: dict = {}
    for _, row in m.iterrows():
        k = norm_key(row.get("key", ""))
        if not k:
            continue
        mean = str(row.get("col_mean", "")).replace("\ufeff", "").strip() if pd.notna(row.get("col_mean", "")) else ""
        sd = str(row.get("col_sd", "")).replace("\ufeff", "").strip() if pd.notna(row.get("col_sd", "")) else ""
        mappings[k] = {"mean": mean, "sd": sd}

    required = {"power_kw", "fuel_kgh", "lhv_kj_kg"}
    missing = required - set(mappings.keys())
    if missing:
        raise KeyError(f"Faltam keys em Mappings: {missing}. Keys lidas: {sorted(mappings.keys())}")
    for rk in required:
        if not mappings[rk]["mean"]:
            raise KeyError(f"Mappings['{rk}'].col_mean está vazio. Preencha a coluna col_mean na aba Mappings.")

    ins = _read_excel(p, sheet_name="Instruments")
    ins.columns = _normalize_cols(list(ins.columns))
    need_i = {"key", "model", "dist", "percent", "digits", "lsd", "abs", "resolution"}
    if not need_i.issubset(set(ins.columns)):
        raise KeyError(f"Aba Instruments precisa de {need_i}. Encontradas: {list(ins.columns)}")

    instruments: dict = {}
    for _, row in ins.iterrows():
        k = norm_key(row.get("key", ""))
        if not k:
            continue
        instruments[k] = {
            "model": str(row.get("model", "")).strip().lower(),
            "dist": str(row.get("dist", "rect")).strip().lower(),
            "percent": _to_float(row.get("percent", 0.0), 0.0),
            "digits": _to_float(row.get("digits", 0.0), 0.0),
            "lsd": _to_float(row.get("lsd", 0.0), 0.0),
            "abs": _to_float(row.get("abs", 0.0), 0.0),
            "resolution": _to_float(row.get("resolution", 0.0), 0.0),
        }

    # defaults (se não tiver linha no excel)
    instruments.setdefault(
        "balance_kg",
        {"model": "resolution_only", "dist": "rect", "resolution": 0.001, "percent": 0.0, "digits": 0.0, "lsd": 0.0, "abs": 0.0},
    )
    instruments.setdefault(
        "power_kw",
        {"model": "percent_plus_digits", "dist": "rect", "percent": 0.01, "digits": 2.0, "lsd": 0.01, "abs": 0.0, "resolution": 0.0},
    )
    instruments.setdefault(
        "lhv_kj_kg",
        {"model": "direct", "dist": "rect", "percent": 0.0, "digits": 0.0, "lsd": 0.0, "abs": 0.0, "resolution": 0.0},
    )

    return mappings, instruments


# -------------------------
# LHV lookup
# -------------------------
def load_lhv_lookup() -> pd.DataFrame:
    p = CFG_DIR / "lhv.csv"
    if not p.exists():
        raise FileNotFoundError(f"Não encontrei {p}.")

    df = pd.read_csv(p, sep=None, engine="python", encoding="utf-8-sig")
    df.columns = _normalize_cols(list(df.columns))

    colmap: Dict[str, str] = {}
    for c in df.columns:
        cl = c.lower().strip()
        if cl in {"etoh_pct", "etoh", "e_pct", "e"}:
            colmap[c] = "EtOH_pct"
        elif cl in {"h2o_pct", "h2o", "h20_pct", "h20", "h_pct", "h"}:
            colmap[c] = "H2O_pct"
        elif cl in {"lhv_kj_kg", "lhv", "pci_kj_kg", "pci"}:
            colmap[c] = "LHV_kJ_kg"
    df = df.rename(columns=colmap)

    for c in ["EtOH_pct", "H2O_pct", "LHV_kJ_kg"]:
        if c not in df.columns:
            raise KeyError(f"lhv.csv precisa da coluna {c}. Colunas atuais: {list(df.columns)}")

    df["EtOH_pct"] = pd.to_numeric(df["EtOH_pct"], errors="coerce").astype("Int64")
    df["H2O_pct"] = pd.to_numeric(df["H2O_pct"], errors="coerce").astype("Int64")
    df["LHV_kJ_kg"] = pd.to_numeric(df["LHV_kJ_kg"], errors="coerce")
    return df


# -------------------------
# Uncertainty Type B models
# -------------------------
def uB_power_kw(P: pd.Series, spec: dict) -> pd.Series:
    """UPD: ±(percent*reading + digits*lsd + abs), retangular por padrão."""
    P = pd.to_numeric(P, errors="coerce")
    limit = abs(spec.get("percent", 0.0)) * P.abs() + abs(spec.get("digits", 0.0)) * abs(spec.get("lsd", 0.0)) + abs(spec.get("abs", 0.0))
    if spec.get("dist", "rect") == "rect":
        return rect_to_std(limit)
    return limit


def uB_direct(value: pd.Series, spec: dict) -> pd.Series:
    """Modelo genérico: ±(abs + percent*value) + resolução."""
    v = pd.to_numeric(value, errors="coerce")
    limit = abs(spec.get("abs", 0.0)) + abs(spec.get("percent", 0.0)) * v.abs()
    u_acc = rect_to_std(limit) if spec.get("dist", "rect") == "rect" else limit
    u_res = res_to_std(abs(spec.get("resolution", 0.0)))
    return (u_acc**2 + u_res**2) ** 0.5


# -------------------------
# Stats
# -------------------------
def compute_trechos_stats(lv_raw: pd.DataFrame, instruments: dict) -> pd.DataFrame:
    """
    Produz nível TRECHO:
      - Consumo_kg_h por janela (delta balança)
      - uB_Consumo_kg_h por janela (resolução balança)
      - Means dentro da janela (vamos manter só o que for necessário no ponto)
    """
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
        return pd.DataFrame(columns=group_cols + ["N_samples", "Consumo_kg_h", "uB_Consumo_kg_h"])

    lv_valid = lv.set_index(group_cols).loc[valid_idx].reset_index()
    gv = lv_valid.groupby(group_cols, dropna=False, sort=True)

    means = gv[candidate_cols].mean(numeric_only=True).add_suffix("_mean")

    first = gv[bcol].first().rename("BEtanol_start")
    last = gv[bcol].last().rename("BEtanol_end")
    n2 = gv.size().rename("N_samples")

    out = pd.concat([means, first, last, n2], axis=1).reset_index()

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

    keep = group_cols + [c for c in out.columns if c.endswith("_mean")] + ["Consumo_kg_h", "uB_Consumo_kg_h", "N_samples"]
    return out[keep].copy()


def compute_ponto_stats(trechos: pd.DataFrame) -> pd.DataFrame:
    """
    Produz nível PONTO:
      - média e desvio padrão ENTRE janelas para cada variável *_mean
      - uB_Consumo_kg_h_mean_of_windows = sqrt(sum(uB_i^2))/N
    """
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
    n_trechos = g.size().rename("N_trechos_validos")

    out = pd.concat([mean_of_windows, sd_of_windows, n_trechos], axis=1).reset_index()

    uB_col = "uB_Consumo_kg_h"
    if uB_col in tre.columns:
        tre_sorted = tre.sort_values(group_cols + ["WindowID"])
        g2 = tre_sorted.groupby(group_cols, dropna=False, sort=True)
        sum_u2 = g2[uB_col].apply(lambda s: float((pd.to_numeric(s, errors="coerce") ** 2).sum()))
        N = n_trechos
        out["uB_Consumo_kg_h_mean_of_windows"] = (sum_u2 ** 0.5) / N.values
    else:
        out["uB_Consumo_kg_h_mean_of_windows"] = pd.NA

    return out.copy()


# -------------------------
# Final KPI table (clean)
# -------------------------
def build_final_table(ponto: pd.DataFrame, lhv: pd.DataFrame, mappings: dict, instruments: dict) -> pd.DataFrame:
    if ponto.empty:
        return pd.DataFrame()

    df = ponto.merge(lhv, on=["EtOH_pct", "H2O_pct"], how="left")

    # resolve colunas principais (mapeadas)
    P_mean = resolve_col(df, mappings["power_kw"]["mean"])
    P_sd_name = mappings["power_kw"]["sd"]
    P_sd = resolve_col(df, P_sd_name) if P_sd_name else ""

    F_mean = resolve_col(df, mappings["fuel_kgh"]["mean"])
    F_sd_name = mappings["fuel_kgh"]["sd"]
    F_sd = resolve_col(df, F_sd_name) if F_sd_name else ""

    L_col = resolve_col(df, mappings["lhv_kj_kg"]["mean"])

    N = pd.to_numeric(df["N_trechos_validos"], errors="coerce")

    # --- Tipo A ---
    df["uA_P_kw"] = pd.to_numeric(df[P_sd], errors="coerce") / (N**0.5) if P_sd else pd.NA
    df["uA_Consumo_kg_h"] = pd.to_numeric(df[F_sd], errors="coerce") / (N**0.5) if F_sd else pd.NA

    # --- Tipo B ---
    df["uB_P_kw"] = uB_power_kw(df[P_mean], instruments.get("power_kw", {}))

    if "uB_Consumo_kg_h_mean_of_windows" in df.columns:
        df["uB_Consumo_kg_h"] = pd.to_numeric(df["uB_Consumo_kg_h_mean_of_windows"], errors="coerce")
    else:
        df["uB_Consumo_kg_h"] = pd.NA

    df["uB_LHV_kJ_kg"] = uB_direct(df[L_col], instruments.get("lhv_kj_kg", {}))

    # --- combinada e expandida (P e consumo) ---
    df["uc_P_kw"] = (df["uA_P_kw"]**2 + df["uB_P_kw"]**2) ** 0.5
    df["U_P_kw"] = K_COVERAGE * df["uc_P_kw"]

    df["uc_Consumo_kg_h"] = (df["uA_Consumo_kg_h"]**2 + df["uB_Consumo_kg_h"]**2) ** 0.5
    df["U_Consumo_kg_h"] = K_COVERAGE * df["uc_Consumo_kg_h"]

    # --- n_th ---
    PkW = pd.to_numeric(df[P_mean], errors="coerce")     # kW = kJ/s
    Fkgh = pd.to_numeric(df[F_mean], errors="coerce")   # kg/h
    mdot = Fkgh / 3600.0                                # kg/s
    LHVv = pd.to_numeric(df[L_col], errors="coerce")    # kJ/kg

    df["n_th"] = PkW / (mdot * LHVv)
    df.loc[(PkW <= 0) | (mdot <= 0) | (LHVv <= 0), "n_th"] = pd.NA
    df["n_th_pct"] = df["n_th"] * 100.0

    # propagação separada A/B
    relA = ((df["uA_P_kw"] / PkW) ** 2 + (df["uA_Consumo_kg_h"] / Fkgh) ** 2) ** 0.5
    relB = ((df["uB_P_kw"] / PkW) ** 2 + (df["uB_Consumo_kg_h"] / Fkgh) ** 2 + (df["uB_LHV_kJ_kg"] / LHVv) ** 2) ** 0.5

    df["uA_n_th"] = df["n_th"] * relA
    df["uB_n_th"] = df["n_th"] * relB
    df["uc_n_th"] = (df["uA_n_th"]**2 + df["uB_n_th"]**2) ** 0.5
    df["U_n_th"] = K_COVERAGE * df["uc_n_th"]
    df["U_n_th_pct"] = df["U_n_th"] * 100.0

    # stats "limpas"
    keep_stats = [c for c in df.columns if c.endswith("_mean_mean_of_windows") or c.endswith("_mean_sd_of_windows")]

    required_cols = [
        "BaseName", "Load_kW", "EtOH_pct", "H2O_pct", "N_trechos_validos",
        P_mean, P_sd if P_sd else None,
        F_mean, F_sd if F_sd else None,
        L_col,
        "n_th", "n_th_pct",
        "uA_P_kw", "uB_P_kw", "uc_P_kw", "U_P_kw",
        "uA_Consumo_kg_h", "uB_Consumo_kg_h", "uc_Consumo_kg_h", "U_Consumo_kg_h",
        "uB_LHV_kJ_kg",
        "uA_n_th", "uB_n_th", "uc_n_th", "U_n_th", "U_n_th_pct",
    ]
    required_cols = [c for c in required_cols if c and c in df.columns]

    base_cols = ["BaseName", "Load_kW", "EtOH_pct", "H2O_pct", "N_trechos_validos"]
    base_cols = [c for c in base_cols if c in df.columns]

    final_cols: List[str] = []
    for c in base_cols + required_cols + keep_stats:
        if c not in final_cols:
            final_cols.append(c)

    return df[final_cols].copy()


# -------------------------
# main
# -------------------------
def main() -> None:
    OUT_DIR.mkdir(exist_ok=True)

    raw_files = [p for p in RAW_DIR.glob("*") if p.is_file() and not p.name.startswith("~$")]
    metas = [parse_meta(p) for p in raw_files]
    lv_files = [m for m in metas if m.source_type == "LABVIEW" and m.path.suffix.lower() == ".xlsx"]
    if not lv_files:
        raise SystemExit("Não achei .xlsx do LabVIEW em raw/.")

    mappings, instruments = load_config_excel()

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

    out = build_final_table(ponto, lhv, mappings, instruments)

    lv_raw.to_parquet(OUT_DIR / "lv_raw.parquet", index=False)
    trechos.to_parquet(OUT_DIR / "lv_trechos.parquet", index=False)
    ponto.to_parquet(OUT_DIR / "lv_ponto.parquet", index=False)

    out_xlsx = safe_to_excel(out, OUT_DIR / "lv_kpis_clean.xlsx")

    print("OK! Gerado:")
    print(" - out/lv_raw.parquet")
    print(" - out/lv_trechos.parquet")
    print(" - out/lv_ponto.parquet")
    print(f" - {out_xlsx} (médias, sd, incertezas e n_th)")


if __name__ == "__main__":
    main()