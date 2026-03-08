from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from datetime import datetime
from math import sqrt
import difflib

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# =========================
# Paths / constants
# =========================
RAW_DIR = Path("raw")
OUT_DIR = Path("out")
PLOTS_DIR = OUT_DIR / "plots"
CFG_DIR = Path("config")

PREFERRED_SHEET_NAME = "labview"
B_ETANOL_COL_CANDIDATES = ["B_Etanol", "B_ETANOL", "B_ETANOL (kg)", "B_Etanol (kg)"]

SAMPLES_PER_WINDOW = 30
MIN_SAMPLES_PER_WINDOW = 30
DT_S = 1.0
K_COVERAGE = 2.0

FUEL_H2O_LEVELS = [6, 25, 35]  # “combustíveis” por hidratação

# =========================
# Airflow assumptions (E94H6 reference)
# =========================
AFR_STOICH_E94H6 = 8.4
ETHANOL_FRAC_E94H6 = 0.94
LAMBDA_DEFAULT = 1.0


# =========================
# Psychrometrics / cp models
# =========================
R_V_WATER = 461.5  # J/(kg*K)
CP_WATER_VAPOR_KJ_KG_K = 1.86  # kJ/(kg*K), engineering approximation


# =========================
# Excel helpers
# =========================
def _excel_engine_preferred() -> str:
    try:
        import python_calamine  # noqa: F401
        return "calamine"
    except Exception:
        return "openpyxl"


def _read_excel(path: Path, sheet_name: str | int | None = 0) -> pd.DataFrame:
    eng = _excel_engine_preferred()
    if eng == "calamine":
        try:
            return pd.read_excel(path, sheet_name=sheet_name, engine="calamine")
        except Exception as e:
            print(f"[WARN] read_excel calamine falhou em {path.name} (sheet={sheet_name}): {e}. Tentando openpyxl...")
    return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")


def _excel_file(path: Path) -> pd.ExcelFile:
    eng = _excel_engine_preferred()
    if eng == "calamine":
        try:
            return pd.ExcelFile(path, engine="calamine")
        except Exception as e:
            print(f"[WARN] ExcelFile calamine falhou em {path.name}: {e}. Tentando openpyxl...")
    return pd.ExcelFile(path, engine="openpyxl")


# =========================
# Generic helpers
# =========================
def norm_key(x: object) -> str:
    return str(x).replace("\ufeff", "").strip().lower()


def _normalize_cols(cols: List[str]) -> List[str]:
    return [str(c).replace("\ufeff", "").strip() for c in cols]


def resolve_col(df: pd.DataFrame, requested: str) -> str:
    requested = str(requested).replace("\ufeff", "").strip()
    if not requested:
        raise KeyError("Nome de coluna solicitado está vazio (verifique Mappings no config).")

    if requested in df.columns:
        return requested

    low_map = {str(c).lower().strip(): c for c in df.columns}
    req_low = requested.lower().strip()
    if req_low in low_map:
        return low_map[req_low]

    suggestion = difflib.get_close_matches(requested, list(df.columns), n=6)
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
    return step / sqrt(12) if step > 0 else 0.0


def _to_float(x: object, default: float = 0.0) -> float:
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

    s = str(x).replace("\ufeff", "").strip()
    if s == "":
        return default
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return default


def _safe_name(name: str) -> str:
    s = re.sub(r"[^A-Za-z0-9_]+", "_", str(name))
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _is_blank_cell(x: object) -> bool:
    if x is None:
        return True
    try:
        if pd.isna(x):
            return True
    except Exception:
        pass
    s = str(x).replace("\ufeff", "").strip()
    return s == "" or s.lower() == "nan"


def _to_str_or_empty(x: object) -> str:
    return "" if _is_blank_cell(x) else str(x).replace("\ufeff", "").strip()


def _find_first_col_by_substrings(df: pd.DataFrame, substrings: List[str]) -> Optional[str]:
    cols = list(df.columns)
    for c in cols:
        cl = str(c).lower()
        ok = True
        for s in substrings:
            if str(s).lower() not in cl:
                ok = False
                break
        if ok:
            return c
    return None


def _find_kibox_col_by_tokens(df: pd.DataFrame, tokens: List[str]) -> Optional[str]:
    want = [str(t).lower().replace("_", "").replace(" ", "") for t in tokens if str(t).strip()]
    if not want:
        return None

    for c in df.columns:
        cs = str(c)
        if not cs.startswith("KIBOX_"):
            continue
        canon = cs.lower().replace("_", "").replace(" ", "")
        ok = True
        for w in want:
            if w not in canon:
                ok = False
                break
        if ok:
            return c
    return None


def _parse_csv_list_ints(x: object) -> Optional[List[int]]:
    if _is_blank_cell(x):
        return None
    s = str(x).replace("\ufeff", "").strip()
    if not s:
        return None
    parts = [p.strip() for p in s.split(",") if p.strip()]
    out: List[int] = []
    for p in parts:
        if p == "":
            continue
        try:
            out.append(int(float(p.replace(",", "."))))
        except Exception:
            continue
    return out if out else None


def _parse_axis_spec(min_v: object, max_v: object, step_v: object) -> Optional[Tuple[float, float, float]]:
    a = _to_float(min_v, default=np.nan)
    b = _to_float(max_v, default=np.nan)
    c = _to_float(step_v, default=np.nan)
    if not (np.isfinite(a) and np.isfinite(b) and np.isfinite(c)):
        return None
    if c <= 0:
        return None
    return (float(a), float(b), float(c))


# =========================
# Reporting rounding helpers (rev2 sheet Reporting_Rounding)
# =========================
def _round_half_up_to_resolution(x: pd.Series, res: float) -> pd.Series:
    s = pd.to_numeric(x, errors="coerce")
    if res <= 0:
        return s
    q = s / res

    pos = q.where(q >= 0)
    neg = q.where(q < 0)

    pos_r = np.floor(pos + 0.5)
    neg_r = np.ceil(neg - 0.5)

    out = q.copy()
    out = out.where(q.isna(), np.nan)
    out = out.where(q < 0, pos_r)
    out = out.where(q >= 0, neg_r)
    return out * res


# =========================
# Derived airflow channels (no row count change)
# =========================
def _ethanol_mass_fraction_from_etoh_pct(etoh_pct: pd.Series) -> pd.Series:
    return pd.to_numeric(etoh_pct, errors="coerce") / 100.0


def add_airflow_channels_inplace(df: pd.DataFrame, lambda_col: str | None = None) -> pd.DataFrame:
    out = df.copy()

    fuel_col = None
    for c in ["Consumo_kg_h_mean_of_windows", "Consumo_kg_h", "Fuel_kg_h", "fuel_kgh_mean_of_windows"]:
        if c in out.columns:
            fuel_col = c
            break
    if fuel_col is None:
        candidates = [c for c in out.columns if "consumo" in c.lower() and "mean_of_windows" in c.lower()]
        fuel_col = candidates[0] if candidates else None

    if fuel_col is None:
        print("[WARN] Airflow: não achei coluna de consumo (kg/h). Pulando canais de ar.")
        return out

    fuel_mix_kg_h = pd.to_numeric(out[fuel_col], errors="coerce")

    x_etoh = _ethanol_mass_fraction_from_etoh_pct(out["EtOH_pct"])
    out["EtOH_pure_mass_frac"] = x_etoh

    out["Fuel_EtOH_pure_kg_h"] = fuel_mix_kg_h * x_etoh
    out["Fuel_E94H6_eq_kg_h"] = out["Fuel_EtOH_pure_kg_h"] / ETHANOL_FRAC_E94H6

    if lambda_col and lambda_col in out.columns:
        out["lambda_used"] = pd.to_numeric(out[lambda_col], errors="coerce")
    else:
        out["lambda_used"] = LAMBDA_DEFAULT

    out["AFR_stoich_E94H6"] = AFR_STOICH_E94H6
    out["AFR_real"] = out["lambda_used"] * out["AFR_stoich_E94H6"]

    out["Air_kg_h"] = out["AFR_real"] * out["Fuel_E94H6_eq_kg_h"]
    out["Air_kg_s"] = out["Air_kg_h"] / 3600.0
    out["Air_g_s"] = out["Air_kg_s"] * 1000.0

    return out


# =========================
# Psychrometrics helpers
# =========================
def _psat_water_pa_magnus(T_C: pd.Series) -> pd.Series:
    T = pd.to_numeric(T_C, errors="coerce")
    es_hpa = 6.112 * np.exp((17.62 * T) / (243.12 + T))
    return es_hpa * 100.0  # Pa


def _humidity_ratio_w_from_rh(T_C: pd.Series, RH_pct: pd.Series, P_kPa_abs: pd.Series) -> pd.Series:
    T = pd.to_numeric(T_C, errors="coerce")
    RH = pd.to_numeric(RH_pct, errors="coerce") / 100.0
    P_pa = pd.to_numeric(P_kPa_abs, errors="coerce") * 1000.0

    psat = _psat_water_pa_magnus(T)
    pv = RH.clip(lower=0.0, upper=1.0) * psat
    pv = pv.where((pv.notna()) & (P_pa.notna()) & (pv < 0.99 * P_pa), pd.NA)

    w = 0.62198 * pv / (P_pa - pv)
    return pd.to_numeric(w, errors="coerce")


def _absolute_humidity_g_m3(T_C: pd.Series, RH_pct: pd.Series) -> pd.Series:
    T = pd.to_numeric(T_C, errors="coerce")
    RH = pd.to_numeric(RH_pct, errors="coerce") / 100.0

    T_K = T + 273.15
    psat = _psat_water_pa_magnus(T)
    pv = RH.clip(lower=0.0, upper=1.0) * psat

    rho_v_kg_m3 = pv / (R_V_WATER * T_K)
    return rho_v_kg_m3 * 1000.0  # g/m^3


def _cp_air_dry_kj_kgk(T_C: pd.Series) -> pd.Series:
    T = pd.to_numeric(T_C, errors="coerce")
    return 1.005 + 0.0001 * (T - 25.0)


def _cp_moist_air_kj_kgk(T_C: pd.Series, RH_pct: pd.Series, P_kPa_abs: pd.Series) -> pd.Series:
    w = _humidity_ratio_w_from_rh(T_C, RH_pct, P_kPa_abs)
    yv = w / (1.0 + w)
    cp_dry = _cp_air_dry_kj_kgk(T_C)
    cp_mix = (1.0 - yv) * cp_dry + yv * CP_WATER_VAPOR_KJ_KG_K
    return pd.to_numeric(cp_mix, errors="coerce")


# =========================
# File meta
# =========================
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

    m_kw = re.search(r"(\d+)\s*[-_ ]?\s*kw", basename, flags=re.IGNORECASE)
    load_kw = int(m_kw.group(1)) if m_kw else None

    m_eh = re.search(r"E(\d+)\s*H(\d+)", basename, flags=re.IGNORECASE)
    etoh_pct = int(m_eh.group(1)) if m_eh else None
    h2o_pct = int(m_eh.group(2)) if m_eh else None

    return FileMeta(path=path, basename=basename, source_type=source_type, load_kw=load_kw, etoh_pct=etoh_pct, h2o_pct=h2o_pct)


# =========================
# LabVIEW read
# =========================
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

    df = df.reset_index(drop=True)
    df["Index"] = range(len(df))
    df["WindowID"] = df["Index"] // SAMPLES_PER_WINDOW

    df = df.assign(BaseName=meta.basename, Load_kW=meta.load_kw, EtOH_pct=meta.etoh_pct, H2O_pct=meta.h2o_pct)

    first_cols = ["BaseName", "Load_kW", "EtOH_pct", "H2O_pct", "Index", "WindowID"]
    rest = [c for c in df.columns if c not in first_cols]
    return df[first_cols + rest].copy()


# =========================
# Kibox robust parsing
# =========================
def _sniff_delimiter(sample: str) -> str:
    candidates = [",", ";", "\t", "|"]
    counts = {d: sample.count(d) for d in candidates}
    return max(counts, key=counts.get)


def _find_header_row(lines: List[str], delim: str, min_cols: int = 6) -> int:
    best_i = 0
    best_cols = 0
    for i, ln in enumerate(lines[:80]):
        cols = ln.split(delim)
        ncols = len(cols)
        if ncols > best_cols:
            best_cols = ncols
            best_i = i
        if ncols >= min_cols and any(ch.isalpha() for ch in ln):
            return i
    return best_i


_num_regex = re.compile(r"[-+]?(\d{1,3}(\.\d{3})+|\d+)([.,]\d+)?")


def _coerce_numeric_series(s: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce")

    x = s.astype(str).str.replace("\ufeff", "", regex=False).str.strip()
    x = x.str.replace("\u00A0", " ", regex=False).str.replace(" ", "", regex=False)

    extracted = x.str.extract(_num_regex)[0]
    extracted = extracted.where(extracted.notna(), None)

    def fix_num(v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = str(v)
        if "," in v and "." in v:
            if v.rfind(",") > v.rfind("."):
                v = v.replace(".", "").replace(",", ".")
            else:
                v = v.replace(",", "")
            return v
        if "," in v and "." not in v:
            return v.replace(",", ".")
        return v

    fixed = extracted.map(fix_num)
    return pd.to_numeric(fixed, errors="coerce")


def read_kibox_csv_robust(path: Path) -> pd.DataFrame:
    text = path.read_text(encoding="utf-8-sig", errors="ignore")
    sample = "\n".join(text.splitlines()[:50])
    delim = _sniff_delimiter(sample)
    lines = text.splitlines()
    header_row = _find_header_row(lines, delim=delim, min_cols=6)

    df = pd.read_csv(path, sep=delim, engine="python", encoding="utf-8-sig", skiprows=header_row)
    df.columns = _normalize_cols(list(df.columns))
    df = df.loc[:, ~pd.Series(df.columns).astype(str).str.startswith("Unnamed").values].copy()
    return df


def kibox_mean_row(meta: FileMeta) -> pd.DataFrame:
    df_raw = read_kibox_csv_robust(meta.path)
    num_df = pd.DataFrame({c: _coerce_numeric_series(df_raw[c]) for c in df_raw.columns})

    keep_cols = [c for c in num_df.columns if num_df[c].notna().mean() >= 0.2]
    if not keep_cols:
        fill = sorted([(c, float(num_df[c].notna().mean())) for c in num_df.columns], key=lambda x: x[1], reverse=True)
        keep_cols = [c for c, _ in fill[:30]]

    means = num_df[keep_cols].mean(numeric_only=True)

    row = {f"KIBOX_{c}": float(means[c]) if pd.notna(means[c]) else pd.NA for c in means.index}
    row.update({"Load_kW": meta.load_kw, "EtOH_pct": meta.etoh_pct, "H2O_pct": meta.h2o_pct})
    return pd.DataFrame([row])


def kibox_aggregate(kibox_files: List[FileMeta]) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    for m in kibox_files:
        if m.load_kw is None or m.etoh_pct is None or m.h2o_pct is None:
            print(f"[WARN] Kibox sem KW/E/H no nome (não vou agregar): {m.path.name}")
            continue
        try:
            rows.append(kibox_mean_row(m))
        except Exception as e:
            print(f"[ERROR] Kibox {m.path.name}: {e}")

    if not rows:
        return pd.DataFrame(columns=["Load_kW", "EtOH_pct", "H2O_pct"])

    allk = pd.concat(rows, ignore_index=True)
    key_cols = ["Load_kW", "EtOH_pct", "H2O_pct"]
    value_cols = [c for c in allk.columns if c.startswith("KIBOX_")]

    agg = allk.groupby(key_cols, dropna=False, sort=True)[value_cols].mean(numeric_only=True).reset_index()
    cnt = allk.groupby(key_cols, dropna=False, sort=True).size().reset_index(name="KIBOX_N_files")
    return agg.merge(cnt, on=key_cols, how="left")


# =========================
# Config / LHV / Instruments rev2 + Plots (rev3-ready)
# =========================
def _choose_config_path() -> Path:
    candidates = [
        CFG_DIR / "config_incertezas_rev3.xlsx",
        CFG_DIR / "config_incertezas_rev3_renamed.xlsx",
        CFG_DIR / "config_incertezas_rev2_renamed.xlsx",
        CFG_DIR / "config_incertezas_rev2.xlsx",
        CFG_DIR / "config_incertezas.xlsx",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(f"Não encontrei nenhum config_incertezas*.xlsx em {CFG_DIR.resolve()}")


def _try_read_sheet(xlsx_path: Path, sheet: str) -> Optional[pd.DataFrame]:
    try:
        xf = _excel_file(xlsx_path)
        if sheet not in xf.sheet_names:
            return None
        return _read_excel(xlsx_path, sheet_name=sheet)
    except Exception:
        return None


def load_config_excel() -> Tuple[dict, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    p = _choose_config_path()

    m = _read_excel(p, sheet_name="Mappings")
    m.columns = _normalize_cols(list(m.columns))

    mappings: dict = {}
    for _, row in m.iterrows():
        k = norm_key(row.get("key", ""))
        if not k:
            continue
        mappings[k] = {
            "mean": str(row.get("col_mean", "")).replace("\ufeff", "").strip(),
            "sd": str(row.get("col_sd", "")).replace("\ufeff", "").strip(),
        }

    required = {"power_kw", "fuel_kgh", "lhv_kj_kg"}
    missing = required - set(mappings.keys())
    if missing:
        raise KeyError(f"Faltam keys em Mappings: {missing}. Keys lidas: {sorted(mappings.keys())}")

    ins = _try_read_sheet(p, "Instruments")
    if ins is None:
        ins = pd.DataFrame()

    ins.columns = _normalize_cols(list(ins.columns))
    ins_cols_low = {c.lower().strip(): c for c in ins.columns}

    is_rev2 = "acc_abs" in ins_cols_low and "acc_pct" in ins_cols_low
    if not is_rev2 and not ins.empty:
        def get_col(name: str) -> Optional[str]:
            return ins_cols_low.get(name.lower().strip())

        key_c = get_col("key")
        dist_c = get_col("dist")
        pct_c = get_col("percent")
        dig_c = get_col("digits")
        lsd_c = get_col("lsd")
        abs_c = get_col("abs")
        res_c = get_col("resolution")
        model_c = get_col("model")

        rows = []
        for _, r in ins.iterrows():
            k = str(r.get(key_c, "")).strip()
            if not k:
                continue
            dist = str(r.get(dist_c, "rect")).strip() if dist_c else "rect"
            pct = _to_float(r.get(pct_c, 0.0), 0.0) if pct_c else 0.0
            dig = _to_float(r.get(dig_c, 0.0), 0.0) if dig_c else 0.0
            lsd = _to_float(r.get(lsd_c, 0.0), 0.0) if lsd_c else 0.0
            absv = _to_float(r.get(abs_c, 0.0), 0.0) if abs_c else 0.0
            res = _to_float(r.get(res_c, 0.0), 0.0) if res_c else 0.0
            model = str(r.get(model_c, "")).strip() if model_c else ""

            rows.append(
                {
                    "key": k,
                    "component": f"{k}_spec",
                    "dist": dist if dist else "rect",
                    "range_min": pd.NA,
                    "range_max": pd.NA,
                    "acc_abs": absv,
                    "acc_pct": pct,
                    "digits": dig,
                    "lsd": lsd,
                    "resolution": res,
                    "source": "",
                    "notes": f"migrated_from_model={model}",
                }
            )
        instruments_df = pd.DataFrame(rows)
    else:
        instruments_df = ins.copy()
        for c in [
            "key",
            "component",
            "dist",
            "range_min",
            "range_max",
            "acc_abs",
            "acc_pct",
            "digits",
            "lsd",
            "resolution",
            "source",
            "notes",
        ]:
            if c not in instruments_df.columns:
                instruments_df[c] = pd.NA

    instruments_df["key_norm"] = instruments_df["key"].map(norm_key)

    rep = _try_read_sheet(p, "Reporting_Rounding")
    if rep is None:
        rep = _try_read_sheet(p, "UPD_Rounding")
    if rep is None:
        rep = pd.DataFrame(columns=["key", "report_resolution", "report_digits", "rule", "notes"])

    rep.columns = _normalize_cols(list(rep.columns))
    if "key" not in rep.columns:
        rep["key"] = pd.NA
    if "report_resolution" not in rep.columns:
        rep["report_resolution"] = pd.NA
    if "rule" not in rep.columns:
        rep["rule"] = "round_half_up"
    rep["key_norm"] = rep["key"].map(norm_key)

    plots = _try_read_sheet(p, "Plots")
    if plots is None:
        plots = pd.DataFrame()

    if not plots.empty:
        plots.columns = _normalize_cols(list(plots.columns))
    else:
        plots = pd.DataFrame(
            columns=[
                "enabled",
                "plot_type",
                "filename",
                "title",
                "x_col",
                "y_col",
                "yerr_col",
                "x_label",
                "y_label",
                "x_min",
                "x_max",
                "x_step",
                "y_min",
                "y_max",
                "y_step",
                "filter_h2o_list",
                "label_variant",
                "notes",
            ]
        )

    return mappings, instruments_df, rep, plots


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


# =========================
# Instruments rev2: uB computation
# =========================
def _has_instrument_key(instruments_df: pd.DataFrame, key_norm: str) -> bool:
    if instruments_df is None or instruments_df.empty:
        return False
    if "key_norm" not in instruments_df.columns:
        return False
    return bool(instruments_df["key_norm"].eq(key_norm).any())


def _get_resolution_for_key(instruments_df: pd.DataFrame, key_norm: str) -> Optional[float]:
    if not _has_instrument_key(instruments_df, key_norm):
        return None
    rows = instruments_df[instruments_df["key_norm"].eq(key_norm)].copy()
    if rows.empty:
        return None
    res = pd.to_numeric(rows.get("resolution", pd.Series([], dtype="float64")), errors="coerce").abs()
    if res.dropna().empty:
        return None
    return float(res.dropna().max())


def uB_from_instruments_rev2(x: pd.Series, key_norm: str, instruments_df: pd.DataFrame) -> pd.Series:
    if instruments_df is None or instruments_df.empty:
        return pd.Series([pd.NA] * len(x), index=x.index)

    if not _has_instrument_key(instruments_df, key_norm):
        return pd.Series([pd.NA] * len(x), index=x.index)

    rows = instruments_df[instruments_df["key_norm"].eq(key_norm)].copy()
    if rows.empty:
        return pd.Series([pd.NA] * len(x), index=x.index)

    xv = pd.to_numeric(x, errors="coerce")
    u2 = pd.Series(0.0, index=xv.index, dtype="float64")

    for _, r in rows.iterrows():
        dist = str(r.get("dist", "rect")).strip().lower() or "rect"

        rmin = r.get("range_min", pd.NA)
        rmax = r.get("range_max", pd.NA)
        rmin_v = _to_float(rmin, default=np.nan)
        rmax_v = _to_float(rmax, default=np.nan)

        mask = pd.Series(True, index=xv.index)
        if np.isfinite(rmin_v):
            mask = mask & (xv >= rmin_v)
        if np.isfinite(rmax_v):
            mask = mask & (xv <= rmax_v)

        acc_abs = _to_float(r.get("acc_abs", 0.0), 0.0)
        acc_pct = _to_float(r.get("acc_pct", 0.0), 0.0)
        digits = _to_float(r.get("digits", 0.0), 0.0)
        lsd = _to_float(r.get("lsd", 0.0), 0.0)
        resolution = _to_float(r.get("resolution", 0.0), 0.0)

        limit = xv.abs() * acc_pct + acc_abs + abs(digits) * abs(lsd)
        limit = limit.where(mask, 0.0)

        if dist == "normal":
            u_acc = limit
        else:
            u_acc = rect_to_std(limit)

        u_res = res_to_std(abs(resolution))
        u_comp = (u_acc**2 + (u_res**2)) ** 0.5

        u2 = u2 + (pd.to_numeric(u_comp, errors="coerce").fillna(0.0) ** 2)

    u = (u2**0.5).where(xv.notna(), pd.NA)
    return u


# =========================
# LabVIEW stats (trechos / ponto)
# =========================
def compute_trechos_stats(lv_raw: pd.DataFrame, instruments_df: pd.DataFrame) -> pd.DataFrame:
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

    bal_key = "balance_kg"
    if _has_instrument_key(instruments_df, bal_key):
        res_kg = _get_resolution_for_key(instruments_df, bal_key)
        if res_kg is None or not np.isfinite(res_kg) or res_kg <= 0:
            out["uB_Consumo_kg_h"] = pd.NA
            print("[WARN] balance_kg existe em Instruments, mas 'resolution' está vazio/ inválido. uB_Consumo_kg_h ficou NA.")
        else:
            u_read = res_to_std(res_kg)  # kg
            u_delta = sqrt(2) * u_read   # kg
            out["uB_Consumo_kg_h"] = (u_delta / out["DeltaT_s"]) * 3600.0
            out.loc[out["DeltaT_s"] <= 0, "uB_Consumo_kg_h"] = pd.NA
    else:
        out["uB_Consumo_kg_h"] = pd.NA

    keep = group_cols + [c for c in out.columns if c.endswith("_mean")] + ["Consumo_kg_h", "uB_Consumo_kg_h", "N_samples"]
    return out[keep].copy()


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
    n_trechos = g.size().rename("N_trechos_validos")

    out = pd.concat([mean_of_windows, sd_of_windows, n_trechos], axis=1).reset_index()

    uB_col = "uB_Consumo_kg_h"
    if uB_col in tre.columns:
        tmp = tre[group_cols + [uB_col]].copy()
        tmp[uB_col] = pd.to_numeric(tmp[uB_col], errors="coerce")

        sum_u2_df = (
            tmp.groupby(group_cols, dropna=False, sort=True)[uB_col]
            .apply(lambda s: float((s**2).sum()))
            .reset_index(name="sum_u2")
        )
        out = out.merge(sum_u2_df, on=group_cols, how="left")

        N = pd.to_numeric(out["N_trechos_validos"], errors="coerce")
        out["uB_Consumo_kg_h_mean_of_windows"] = (pd.to_numeric(out["sum_u2"], errors="coerce") ** 0.5) / N
        out.drop(columns=["sum_u2"], inplace=True)
    else:
        out["uB_Consumo_kg_h_mean_of_windows"] = pd.NA

    return out.copy()


# =========================
# Uncertainty workflow (generic, mapping-driven)
# =========================
def _prefix_from_key_norm(key_norm: str) -> str:
    if key_norm == "power_kw":
        return "P_kw"
    if key_norm == "fuel_kgh":
        return "Consumo_kg_h"
    if key_norm == "lhv_kj_kg":
        return "LHV_kJ_kg"
    return key_norm.upper()


def add_uncertainties_from_mappings(
    df: pd.DataFrame,
    mappings: dict,
    instruments_df: pd.DataFrame,
    N: pd.Series,
) -> pd.DataFrame:
    out = df.copy()

    for key_norm, spec in mappings.items():
        col_mean_req = str(spec.get("mean", "")).strip()
        if not col_mean_req:
            continue

        try:
            col_mean = resolve_col(out, col_mean_req)
        except Exception as e:
            print(f"[WARN] Uncertainty: key='{key_norm}' col_mean '{col_mean_req}' não encontrada no output. Pulando. ({e})")
            continue

        col_sd_req = str(spec.get("sd", "")).strip()
        col_sd = None
        if col_sd_req:
            try:
                col_sd = resolve_col(out, col_sd_req)
            except Exception:
                col_sd = None

        prefix = _prefix_from_key_norm(key_norm)

        if col_sd is not None and col_sd in out.columns:
            out[f"uA_{prefix}"] = pd.to_numeric(out[col_sd], errors="coerce") / (pd.to_numeric(N, errors="coerce") ** 0.5)
        else:
            out[f"uA_{prefix}"] = pd.NA

        out[f"uB_{prefix}"] = uB_from_instruments_rev2(pd.to_numeric(out[col_mean], errors="coerce"), key_norm=key_norm, instruments_df=instruments_df)

        ua = pd.to_numeric(out[f"uA_{prefix}"], errors="coerce")
        ub = pd.to_numeric(out[f"uB_{prefix}"], errors="coerce")
        out[f"uc_{prefix}"] = (ua**2 + ub**2) ** 0.5
        out[f"U_{prefix}"] = K_COVERAGE * out[f"uc_{prefix}"]

    return out


def _apply_reporting_rounding(df: pd.DataFrame, mappings: dict, reporting_df: pd.DataFrame) -> pd.DataFrame:
    if reporting_df is None or reporting_df.empty:
        return df

    out = df.copy()
    for _, r in reporting_df.iterrows():
        key_norm = norm_key(r.get("key", ""))
        if not key_norm:
            continue
        if key_norm not in mappings:
            continue

        col_mean = str(mappings[key_norm].get("mean", "")).strip()
        if not col_mean:
            continue
        if col_mean not in out.columns:
            try:
                col_mean = resolve_col(out, col_mean)
            except Exception:
                continue

        res = _to_float(r.get("report_resolution", 0.0), 0.0)
        if res <= 0:
            continue

        rule = str(r.get("rule", "round_half_up")).strip().lower()
        new_col = f"{col_mean}_report"
        if new_col in out.columns:
            continue

        if rule == "round_half_up":
            out[new_col] = _round_half_up_to_resolution(out[col_mean], res)
        else:
            v = pd.to_numeric(out[col_mean], errors="coerce")
            out[new_col] = (np.round(v / res) * res)

    return out


# =========================
# Final table
# =========================
def build_final_table(
    ponto: pd.DataFrame,
    lhv: pd.DataFrame,
    kibox_agg: pd.DataFrame,
    mappings: dict,
    instruments_df: pd.DataFrame,
    reporting_df: pd.DataFrame,
) -> pd.DataFrame:
    df = ponto.merge(lhv, on=["EtOH_pct", "H2O_pct"], how="left")
    if kibox_agg is not None and not kibox_agg.empty:
        df = df.merge(kibox_agg, on=["Load_kW", "EtOH_pct", "H2O_pct"], how="left")

    kibox_bug_cols = ["KIBOX_MBF_10_90_1", "KIBOX_MBF_10_90_AVG_1"]
    drop_now = [c for c in kibox_bug_cols if c in df.columns]
    if drop_now:
        df = df.drop(columns=drop_now)

    ai90_col = _find_kibox_col_by_tokens(df, ["ai", "90"])
    ai10_col = _find_kibox_col_by_tokens(df, ["ai", "10"])
    if ai90_col and ai10_col:
        df["MFB_10_90"] = pd.to_numeric(df[ai90_col], errors="coerce") - pd.to_numeric(df[ai10_col], errors="coerce")
    else:
        df["MFB_10_90"] = pd.NA
        if not ai90_col or not ai10_col:
            print(f"[WARN] Não calculei MFB_10_90: ai90_col={ai90_col}, ai10_col={ai10_col}")

    N = pd.to_numeric(df["N_trechos_validos"], errors="coerce")

    df = add_uncertainties_from_mappings(df, mappings=mappings, instruments_df=instruments_df, N=N)

    if "uB_Consumo_kg_h" in df.columns:
        df["uB_Consumo_kg_h_instrument"] = df["uB_Consumo_kg_h"]
    else:
        df["uB_Consumo_kg_h_instrument"] = pd.NA

    df["uB_Consumo_kg_h"] = pd.to_numeric(df.get("uB_Consumo_kg_h_mean_of_windows", pd.NA), errors="coerce")

    if "uA_Consumo_kg_h" in df.columns:
        df["uc_Consumo_kg_h"] = (pd.to_numeric(df["uA_Consumo_kg_h"], errors="coerce") ** 2 + pd.to_numeric(df["uB_Consumo_kg_h"], errors="coerce") ** 2) ** 0.5
        df["U_Consumo_kg_h"] = K_COVERAGE * df["uc_Consumo_kg_h"]
    else:
        df["uc_Consumo_kg_h"] = pd.NA
        df["U_Consumo_kg_h"] = pd.NA

    P_mean = resolve_col(df, mappings["power_kw"]["mean"])
    F_mean = resolve_col(df, mappings["fuel_kgh"]["mean"])
    L_col = resolve_col(df, mappings["lhv_kj_kg"]["mean"])

    PkW = pd.to_numeric(df[P_mean], errors="coerce")
    Fkgh = pd.to_numeric(df[F_mean], errors="coerce")
    mdot = Fkgh / 3600.0
    LHVv = pd.to_numeric(df[L_col], errors="coerce")

    df["n_th"] = PkW / (mdot * LHVv)
    df.loc[(PkW <= 0) | (mdot <= 0) | (LHVv <= 0), "n_th"] = pd.NA
    df["n_th_pct"] = df["n_th"] * 100.0

    ucP = pd.to_numeric(df.get("uc_P_kw", pd.NA), errors="coerce")
    ucF = pd.to_numeric(df.get("uc_Consumo_kg_h", pd.NA), errors="coerce")
    uBL = pd.to_numeric(df.get("uB_LHV_kJ_kg", pd.NA), errors="coerce")

    rel_uc = ((ucP / PkW) ** 2 + (ucF / Fkgh) ** 2 + (uBL / LHVv) ** 2) ** 0.5
    df["uc_n_th"] = df["n_th"] * rel_uc
    df["U_n_th"] = K_COVERAGE * df["uc_n_th"]
    df["U_n_th_pct"] = df["U_n_th"] * 100.0

    lambda_col = None
    if "lambda" in mappings and mappings["lambda"].get("mean"):
        try:
            lambda_col = resolve_col(df, mappings["lambda"]["mean"])
        except Exception:
            lambda_col = None
    df = add_airflow_channels_inplace(df, lambda_col=lambda_col)

    t_cil_cols = [
        "T_S_CIL_1_mean_mean_of_windows",
        "T_S_CIL_2_mean_mean_of_windows",
        "T_S_CIL_3_mean_mean_of_windows",
        "T_S_CIL_4_mean_mean_of_windows",
    ]
    t_cil_existing = [c for c in t_cil_cols if c in df.columns]
    if t_cil_existing:
        tmp = df[t_cil_existing].apply(pd.to_numeric, errors="coerce")
        df["T_E_CIL_AVG_mean_of_windows"] = tmp.mean(axis=1)
    else:
        df["T_E_CIL_AVG_mean_of_windows"] = pd.NA

    t_adm_col = _find_first_col_by_substrings(df, ["t", "admiss"])
    p_col = _find_first_col_by_substrings(df, ["p", "coletor"])
    rh_col = _find_first_col_by_substrings(df, ["umidade"])

    if t_adm_col and rh_col:
        df["UMIDADE_ABS_g_m3"] = _absolute_humidity_g_m3(df[t_adm_col], df[rh_col])
    else:
        df["UMIDADE_ABS_g_m3"] = pd.NA

    if t_adm_col and rh_col and p_col:
        df["cp_air_dry_kJ_kgK"] = _cp_air_dry_kj_kgk(df[t_adm_col])
        df["cp_air_moist_kJ_kgK"] = _cp_moist_air_kj_kgk(df[t_adm_col], df[rh_col], df[p_col])
        df["hum_ratio_w_kgkg"] = _humidity_ratio_w_from_rh(df[t_adm_col], df[rh_col], df[p_col])
    else:
        df["cp_air_dry_kJ_kgK"] = pd.NA
        df["cp_air_moist_kJ_kgK"] = pd.NA
        df["hum_ratio_w_kgkg"] = pd.NA

    if t_adm_col and "T_E_CIL_AVG_mean_of_windows" in df.columns:
        Tin = pd.to_numeric(df[t_adm_col], errors="coerce")
        Tout = pd.to_numeric(df["T_E_CIL_AVG_mean_of_windows"], errors="coerce")
        df["DT_ADMISSAO_TO_T_E_CIL_AVG_C"] = Tout - Tin
    else:
        df["DT_ADMISSAO_TO_T_E_CIL_AVG_C"] = pd.NA

    if "Air_kg_h" in df.columns and t_adm_col and "T_E_CIL_AVG_mean_of_windows" in df.columns:
        mdot_air = pd.to_numeric(df["Air_kg_h"], errors="coerce") / 3600.0
        dT = pd.to_numeric(df["DT_ADMISSAO_TO_T_E_CIL_AVG_C"], errors="coerce")

        cp_used = pd.to_numeric(df["cp_air_moist_kJ_kgK"], errors="coerce")
        cp_fallback = pd.to_numeric(df["cp_air_dry_kJ_kgK"], errors="coerce")
        cp_used = cp_used.where(cp_used.notna(), cp_fallback)
        cp_used = cp_used.where(cp_used.notna(), 1.005)

        df["Q_EVAP_NET_kW"] = mdot_air * cp_used * dT
    else:
        df["Q_EVAP_NET_kW"] = pd.NA

    df = _apply_reporting_rounding(df, mappings=mappings, reporting_df=reporting_df)
    return df


# =========================
# Plot primitives
# =========================
def plot_all_fuels(
    df: pd.DataFrame,
    y_col: str,
    yerr_col: Optional[str],
    title: str,
    filename: str,
    y_label: str,
    fixed_y: Optional[Tuple[float, float, float]] = None,
    fixed_x: Optional[Tuple[float, float, float]] = None,
    x_col: str = "Load_kW",
    x_label: str = "Power (kW)",
    fuels_override: Optional[List[int]] = None,
) -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    plt.figure()
    any_curve = False

    fuels = fuels_override if fuels_override is not None else FUEL_H2O_LEVELS

    for h in fuels:
        d = df[df["H2O_pct"].astype("Int64") == h].copy()
        d[x_col] = pd.to_numeric(d[x_col], errors="coerce")
        d[y_col] = pd.to_numeric(d[y_col], errors="coerce")
        if yerr_col:
            d[yerr_col] = pd.to_numeric(d[yerr_col], errors="coerce")
            d = d.dropna(subset=[x_col, y_col, yerr_col]).sort_values(x_col)
        else:
            d = d.dropna(subset=[x_col, y_col]).sort_values(x_col)

        if d.empty:
            continue

        any_curve = True
        if yerr_col:
            plt.errorbar(d[x_col], d[y_col], yerr=d[yerr_col], fmt="o-", capsize=3, label=f"H2O={h}%")
        else:
            plt.plot(d[x_col], d[y_col], "o-", label=f"H2O={h}%")

    if not any_curve:
        plt.close()
        print(f"[WARN] Sem dados para plot {filename}")
        return

    if fixed_x is not None:
        xmin, xmax, xstep = fixed_x
        plt.xlim(xmin, xmax)
        try:
            ticks = np.arange(xmin, xmax + 1e-12, xstep).tolist()
            plt.xticks(ticks)
        except Exception:
            pass

    if fixed_y is not None:
        ymin, ymax, ystep = fixed_y
        plt.ylim(ymin, ymax)
        try:
            ticks = np.arange(ymin, ymax + 1e-12, ystep).tolist()
            plt.yticks(ticks)
        except Exception:
            pass

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    plt.legend()
    outpath = PLOTS_DIR / filename
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()
    print(f"[OK] Salvei {outpath}")


def plot_all_fuels_xy(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    yerr_col: Optional[str],
    title: str,
    filename: str,
    x_label: str,
    y_label: str,
    fixed_y: Optional[Tuple[float, float, float]] = None,
    fixed_x: Optional[Tuple[float, float, float]] = None,
    fuels_override: Optional[List[int]] = None,
) -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    plt.figure()
    any_curve = False

    fuels = fuels_override if fuels_override is not None else FUEL_H2O_LEVELS

    for h in fuels:
        d = df[df["H2O_pct"].astype("Int64") == h].copy()
        d[x_col] = pd.to_numeric(d[x_col], errors="coerce")
        d[y_col] = pd.to_numeric(d[y_col], errors="coerce")
        if yerr_col:
            d[yerr_col] = pd.to_numeric(d[yerr_col], errors="coerce")
            d = d.dropna(subset=[x_col, y_col, yerr_col]).sort_values(x_col)
        else:
            d = d.dropna(subset=[x_col, y_col]).sort_values(x_col)

        if d.empty:
            continue

        any_curve = True
        if yerr_col:
            plt.errorbar(d[x_col], d[y_col], yerr=d[yerr_col], fmt="o-", capsize=3, label=f"H2O={h}%")
        else:
            plt.plot(d[x_col], d[y_col], "o-", label=f"H2O={h}%")

    if not any_curve:
        plt.close()
        print(f"[WARN] Sem dados para plot {filename}")
        return

    if fixed_x is not None:
        xmin, xmax, xstep = fixed_x
        plt.xlim(xmin, xmax)
        try:
            ticks = np.arange(xmin, xmax + 1e-12, xstep).tolist()
            plt.xticks(ticks)
        except Exception:
            pass

    if fixed_y is not None:
        ymin, ymax, ystep = fixed_y
        plt.ylim(ymin, ymax)
        try:
            ticks = np.arange(ymin, ymax + 1e-12, ystep).tolist()
            plt.yticks(ticks)
        except Exception:
            pass

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    plt.legend()
    outpath = PLOTS_DIR / filename
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()
    print(f"[OK] Salvei {outpath}")


def _annotate_points_variants(ax, x: np.ndarray, y: np.ndarray, variant: str) -> None:
    for xi, yi in zip(x, y):
        if not np.isfinite(xi) or not np.isfinite(yi):
            continue
        txt = f"{yi:.2f}"
        if variant == "box":
            ax.text(xi, yi, txt, fontsize=8, ha="left", va="bottom",
                    bbox=dict(boxstyle="round,pad=0.25", fc="white", ec="black", lw=0.6))
        elif variant == "tag":
            ax.annotate(txt, xy=(xi, yi), xytext=(6, 6), textcoords="offset points",
                        fontsize=8, ha="left", va="bottom",
                        bbox=dict(boxstyle="round,pad=0.25", fc="white", ec="black", lw=0.6),
                        arrowprops=dict(arrowstyle="->", lw=0.6))
        elif variant == "marker":
            ax.text(xi, yi, txt, fontsize=8, ha="center", va="bottom")
        elif variant == "badge":
            ax.text(xi, yi, txt, fontsize=8, ha="center", va="center",
                    bbox=dict(boxstyle="round,pad=0.22", fc="white", ec="black", lw=0.6, alpha=0.75))
        else:
            ax.text(xi, yi, txt, fontsize=8, ha="left", va="bottom")


def plot_all_fuels_with_value_labels(
    df: pd.DataFrame,
    y_col: str,
    title: str,
    filename: str,
    y_label: str,
    label_variant: str = "box",
    fixed_y: Optional[Tuple[float, float, float]] = None,
    fixed_x: Optional[Tuple[float, float, float]] = None,
    x_col: str = "Load_kW",
    x_label: str = "Power (kW)",
    fuels_override: Optional[List[int]] = None,
) -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots()
    any_curve = False

    fuels = fuels_override if fuels_override is not None else FUEL_H2O_LEVELS

    for h in fuels:
        d = df[df["H2O_pct"].astype("Int64") == h].copy()
        d[x_col] = pd.to_numeric(d[x_col], errors="coerce")
        d[y_col] = pd.to_numeric(d[y_col], errors="coerce")
        d = d.dropna(subset=[x_col, y_col]).sort_values(x_col)

        if d.empty:
            continue

        any_curve = True
        ax.plot(d[x_col], d[y_col], "o-", label=f"H2O={h}%")

        x = pd.to_numeric(d[x_col], errors="coerce").values.astype(float)
        y = pd.to_numeric(d[y_col], errors="coerce").values.astype(float)
        _annotate_points_variants(ax, x, y, label_variant)

    if not any_curve:
        plt.close(fig)
        print(f"[WARN] Sem dados para plot {filename}")
        return

    if fixed_x is not None:
        xmin, xmax, xstep = fixed_x
        ax.set_xlim(xmin, xmax)
        try:
            ticks = np.arange(xmin, xmax + 1e-12, xstep).tolist()
            ax.set_xticks(ticks)
        except Exception:
            pass

    if fixed_y is not None:
        ymin, ymax, ystep = fixed_y
        ax.set_ylim(ymin, ymax)
        try:
            ticks = np.arange(ymin, ymax + 1e-12, ystep).tolist()
            ax.set_yticks(ticks)
        except Exception:
            pass

    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_title(title)
    ax.grid(True, which="both", linestyle="--", linewidth=0.5)
    ax.legend()

    outpath = PLOTS_DIR / filename
    fig.tight_layout()
    fig.savefig(outpath, dpi=220)
    plt.close(fig)
    print(f"[OK] Salvei {outpath}")


# =========================
# Plots-config dispatcher
# =========================
def _row_enabled(v: object) -> bool:
    if v is None:
        return False
    try:
        if pd.isna(v):
            return False
    except Exception:
        pass
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    try:
        return bool(int(float(s)))
    except Exception:
        return False


def _derive_filename_for_expansion(template: str, y_col: str) -> str:
    t = (template or "").strip()
    if not t:
        return f"kibox_{_safe_name(y_col)}_vs_power_all.png"
    if "{y}" in t:
        return t.replace("{y}", _safe_name(y_col))
    if t.lower().endswith(".png"):
        stem = t[:-4]
        return f"{stem}_{_safe_name(y_col)}.png"
    return f"{t}_{_safe_name(y_col)}.png"


def _derive_title_for_expansion(template: str, x_col: str, y_col: str) -> str:
    t = (template or "").strip()
    if not t:
        return f"{y_col} vs {x_col} (all fuels)"
    if "{y}" in t or "{x}" in t:
        return t.replace("{y}", y_col).replace("{x}", x_col)
    return t


def make_plots_from_config(out_df: pd.DataFrame, plots_df: pd.DataFrame) -> None:
    """
    Config-driven plotting (rev3):
      - Each row defines one plot.
      - plot_type supports:
          * all_fuels_yx      -> plot_all_fuels
          * all_fuels_xy      -> plot_all_fuels_xy
          * all_fuels_labels  -> plot_all_fuels_with_value_labels
          * kibox_all         -> expands into one plot per KIBOX_* column (except KIBOX_N_files)
    Notes:
      - Empty cells from Excel come as NaN; we treat them as empty (no more 'nan' column lookup).
      - Missing yerr_col: INFO (plot still works).
      - Missing required columns (y_col or x_col when required): ERROR and skip that plot.
    """
    if plots_df is None or plots_df.empty:
        print("[WARN] Plots config vazio; não gerei plots via planilha.")
        return

    n_ok = 0
    n_skip = 0

    for _, r in plots_df.iterrows():
        if not _row_enabled(r.get("enabled", 0)):
            continue

        plot_type = _to_str_or_empty(r.get("plot_type", ""))
        filename = _to_str_or_empty(r.get("filename", ""))
        title = _to_str_or_empty(r.get("title", ""))

        if not plot_type:
            print("[ERROR] Plots row inválida: plot_type vazio. Pulei.")
            n_skip += 1
            continue

        x_col_req = _to_str_or_empty(r.get("x_col", ""))
        y_col_req = _to_str_or_empty(r.get("y_col", ""))
        yerr_req = _to_str_or_empty(r.get("yerr_col", ""))

        x_label = _to_str_or_empty(r.get("x_label", ""))
        y_label = _to_str_or_empty(r.get("y_label", ""))

        fixed_x = _parse_axis_spec(r.get("x_min", pd.NA), r.get("x_max", pd.NA), r.get("x_step", pd.NA))
        fixed_y = _parse_axis_spec(r.get("y_min", pd.NA), r.get("y_max", pd.NA), r.get("y_step", pd.NA))

        fuels = _parse_csv_list_ints(r.get("filter_h2o_list", pd.NA))
        fuels_override = fuels if fuels is not None else None

        label_variant = _to_str_or_empty(r.get("label_variant", "box")).lower() or "box"

        pt = plot_type.lower().strip()

        # ---------
        # Expansion: Kibox (all columns)
        # ---------
        if pt in {"kibox_all", "all_kibox"}:
            kibox_cols = [c for c in out_df.columns if str(c).startswith("KIBOX_") and c != "KIBOX_N_files"]
            if not kibox_cols:
                print("[WARN] kibox_all: não há colunas KIBOX_* no output. Pulei expansão.")
                n_skip += 1
                continue

            # x column default for kibox_all
            x_col_base = "Load_kW" if not x_col_req else x_col_req
            try:
                x_col = resolve_col(out_df, x_col_base)
            except Exception as e:
                print(f"[ERROR] kibox_all: x_col '{x_col_base}' não encontrado. Pulei expansão. ({e})")
                n_skip += 1
                continue

            xlab = x_label if x_label else x_col

            for yc in sorted(kibox_cols):
                fn = _derive_filename_for_expansion(filename, yc)
                tt = _derive_title_for_expansion(title, x_col=x_col, y_col=yc)
                ylab = y_label if y_label else yc

                plot_all_fuels(
                    out_df,
                    y_col=yc,
                    yerr_col=None,
                    title=tt,
                    filename=fn,
                    y_label=ylab,
                    fixed_y=fixed_y,
                    fixed_x=fixed_x,
                    x_col=x_col,
                    x_label=xlab,
                    fuels_override=fuels_override,
                )
                n_ok += 1
            continue

        # ---------
        # Normal plots (one output per row)
        # ---------
        if pt in {"all_fuels_yx", "all_fuels", "all_fuels_y_vs_x"}:
            x_col_base = "Load_kW" if not x_col_req else x_col_req
            try:
                x_col = resolve_col(out_df, x_col_base)
            except Exception as e:
                print(f"[ERROR] Plot '{filename or title}': x_col '{x_col_base}' não encontrado. Pulei. ({e})")
                n_skip += 1
                continue

            if not y_col_req:
                print(f"[ERROR] Plot '{filename or title}': y_col vazio. Pulei.")
                n_skip += 1
                continue
            try:
                y_col = resolve_col(out_df, y_col_req)
            except Exception as e:
                print(f"[ERROR] Plot '{filename or title}': y_col '{y_col_req}' não encontrado. Pulei. ({e})")
                n_skip += 1
                continue

            yerr_col: Optional[str] = None
            if yerr_req:
                try:
                    yerr_col = resolve_col(out_df, yerr_req)
                except Exception:
                    yerr_col = None
                    print(f"[INFO] Plot '{filename or title}': yerr_col '{yerr_req}' não encontrado. Vou plotar sem erro.")

            if not x_label:
                x_label = x_col
            if not y_label:
                y_label = y_col
            if not title:
                title = f"{y_col} vs {x_col} (all fuels)"
            if not filename:
                filename = f"{_safe_name(y_col)}_vs_{_safe_name(x_col)}_all.png"

            plot_all_fuels(
                out_df,
                y_col=y_col,
                yerr_col=yerr_col,
                title=title,
                filename=filename,
                y_label=y_label,
                fixed_y=fixed_y,
                fixed_x=fixed_x,
                x_col=x_col,
                x_label=x_label,
                fuels_override=fuels_override,
            )
            n_ok += 1
            continue

        if pt in {"all_fuels_xy", "xy"}:
            if not x_col_req:
                print(f"[ERROR] Plot '{filename or title}': x_col vazio (plot_type=all_fuels_xy). Pulei.")
                n_skip += 1
                continue
            if not y_col_req:
                print(f"[ERROR] Plot '{filename or title}': y_col vazio (plot_type=all_fuels_xy). Pulei.")
                n_skip += 1
                continue

            try:
                x_col = resolve_col(out_df, x_col_req)
            except Exception as e:
                print(f"[ERROR] Plot '{filename or title}': x_col '{x_col_req}' não encontrado. Pulei. ({e})")
                n_skip += 1
                continue

            try:
                y_col = resolve_col(out_df, y_col_req)
            except Exception as e:
                print(f"[ERROR] Plot '{filename or title}': y_col '{y_col_req}' não encontrado. Pulei. ({e})")
                n_skip += 1
                continue

            yerr_col = None
            if yerr_req:
                try:
                    yerr_col = resolve_col(out_df, yerr_req)
                except Exception:
                    yerr_col = None
                    print(f"[INFO] Plot '{filename or title}': yerr_col '{yerr_req}' não encontrado. Vou plotar sem erro.")

            if not x_label:
                x_label = x_col
            if not y_label:
                y_label = y_col
            if not title:
                title = f"{y_col} vs {x_col} (all fuels)"
            if not filename:
                filename = f"{_safe_name(y_col)}_vs_{_safe_name(x_col)}_all.png"

            plot_all_fuels_xy(
                out_df,
                x_col=x_col,
                y_col=y_col,
                yerr_col=yerr_col,
                title=title,
                filename=filename,
                x_label=x_label,
                y_label=y_label,
                fixed_y=fixed_y,
                fixed_x=fixed_x,
                fuels_override=fuels_override,
            )
            n_ok += 1
            continue

        if pt in {"all_fuels_labels", "labels"}:
            x_col_base = "Load_kW" if not x_col_req else x_col_req
            try:
                x_col = resolve_col(out_df, x_col_base)
            except Exception as e:
                print(f"[ERROR] Plot '{filename or title}': x_col '{x_col_base}' não encontrado. Pulei. ({e})")
                n_skip += 1
                continue

            if not y_col_req:
                print(f"[ERROR] Plot '{filename or title}': y_col vazio (plot_type=all_fuels_labels). Pulei.")
                n_skip += 1
                continue
            try:
                y_col = resolve_col(out_df, y_col_req)
            except Exception as e:
                print(f"[ERROR] Plot '{filename or title}': y_col '{y_col_req}' não encontrado. Pulei. ({e})")
                n_skip += 1
                continue

            if not x_label:
                x_label = x_col
            if not y_label:
                y_label = y_col
            if not title:
                title = f"{y_col} vs {x_col} (labels)"
            if not filename:
                filename = f"{_safe_name(y_col)}_vs_{_safe_name(x_col)}_labels.png"

            plot_all_fuels_with_value_labels(
                out_df,
                y_col=y_col,
                title=title,
                filename=filename,
                y_label=y_label,
                label_variant=label_variant,
                fixed_y=fixed_y,
                fixed_x=fixed_x,
                x_col=x_col,
                x_label=x_label,
                fuels_override=fuels_override,
            )
            n_ok += 1
            continue

        print(f"[ERROR] Plot '{filename or title}': plot_type '{plot_type}' não suportado. Pulei.")
        n_skip += 1

    print(f"[OK] Plots-config: {n_ok} gerados; {n_skip} pulados.")


# =========================
# Main
# =========================
def main() -> None:
    OUT_DIR.mkdir(exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    raw_files = [p for p in RAW_DIR.glob("*") if p.is_file() and not p.name.startswith("~$")]
    metas = [parse_meta(p) for p in raw_files]

    lv_files = [m for m in metas if m.source_type == "LABVIEW" and m.path.suffix.lower() == ".xlsx"]
    kibox_files = [m for m in metas if m.source_type == "KIBOX" and m.path.suffix.lower() == ".csv"]

    if not lv_files:
        raise SystemExit("Não achei .xlsx do LabVIEW em raw/.")

    mappings, instruments_df, reporting_df, plots_df = load_config_excel()

    lv_all: List[pd.DataFrame] = []
    for m in lv_files:
        try:
            df_i = read_labview_xlsx(m)
            if not df_i.empty:
                lv_all.append(df_i)
        except Exception as e:
            print(f"[ERROR] Falha lendo LabVIEW {m.path.name}: {e}")

    if not lv_all:
        raise SystemExit("Nenhum arquivo LabVIEW foi lido com sucesso.")

    lv_raw = pd.concat(lv_all, ignore_index=True)

    trechos = compute_trechos_stats(lv_raw, instruments_df=instruments_df)
    ponto = compute_ponto_stats(trechos)

    lhv = load_lhv_lookup()
    kibox_agg = kibox_aggregate(kibox_files) if kibox_files else pd.DataFrame(columns=["Load_kW", "EtOH_pct", "H2O_pct"])

    out = build_final_table(ponto, lhv, kibox_agg, mappings, instruments_df, reporting_df)

    out_xlsx = safe_to_excel(out, OUT_DIR / "lv_kpis_clean.xlsx")
    print(f"[OK] Excel gerado: {out_xlsx}")

    make_plots_from_config(out, plots_df)

    if kibox_files:
        print("[INFO] Kibox csv em raw/ detectado. (Histogramas KPEAK continuam fora do workflow por enquanto.)")
    else:
        print("[WARN] Sem Kibox csv em raw/.")


if __name__ == "__main__":
    main()