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

FUEL_H2O_LEVELS = [6, 25, 35]  # “combustíveis” no seu contexto


# -------------------------
# excel engine helper
# -------------------------
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
        except Exception:
            pass
    return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")


def _excel_file(path: Path) -> pd.ExcelFile:
    eng = _excel_engine_preferred()
    if eng == "calamine":
        try:
            return pd.ExcelFile(path, engine="calamine")
        except Exception:
            pass
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

    m_kw = re.search(r"(\d+)\s*[-_ ]?\s*kw", basename, flags=re.IGNORECASE)
    load_kw = int(m_kw.group(1)) if m_kw else None

    m_eh = re.search(r"E(\d+)\s*H(\d+)", basename, flags=re.IGNORECASE)
    etoh_pct = int(m_eh.group(1)) if m_eh else None
    h2o_pct = int(m_eh.group(2)) if m_eh else None

    return FileMeta(path=path, basename=basename, source_type=source_type, load_kw=load_kw, etoh_pct=etoh_pct, h2o_pct=h2o_pct)


# -------------------------
# LabVIEW xlsx read
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
    raise KeyError(f"Não encontrei coluna de balança. Procurei: {B_ETANOL_COL_CANDIDATES}. Colunas: {list(df.columns)[:40]}")


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


# -------------------------
# Kibox robust parsing
# -------------------------
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


# -------------------------
# config / LHV / Type B
# -------------------------
def load_config_excel() -> Tuple[dict, dict]:
    p = CFG_DIR / "config_incertezas.xlsx"
    if not p.exists():
        raise FileNotFoundError(f"Não encontrei {p}.")

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

    ins = _read_excel(p, sheet_name="Instruments")
    ins.columns = _normalize_cols(list(ins.columns))
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

    instruments.setdefault("balance_kg", {"model": "resolution_only", "dist": "rect", "resolution": 0.001, "percent": 0.0, "digits": 0.0, "lsd": 0.0, "abs": 0.0})
    instruments.setdefault("power_kw", {"model": "percent_plus_digits", "dist": "rect", "percent": 0.01, "digits": 2.0, "lsd": 0.01, "abs": 0.0, "resolution": 0.0})
    instruments.setdefault("lhv_kj_kg", {"model": "direct", "dist": "rect", "percent": 0.0, "digits": 0.0, "lsd": 0.0, "abs": 0.0, "resolution": 0.0})
    return mappings, instruments


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

    df["EtOH_pct"] = pd.to_numeric(df["EtOH_pct"], errors="coerce").astype("Int64")
    df["H2O_pct"] = pd.to_numeric(df["H2O_pct"], errors="coerce").astype("Int64")
    df["LHV_kJ_kg"] = pd.to_numeric(df["LHV_kJ_kg"], errors="coerce")
    return df


def uB_power_kw(P: pd.Series, spec: dict) -> pd.Series:
    P = pd.to_numeric(P, errors="coerce")
    limit = abs(spec.get("percent", 0.0)) * P.abs() + abs(spec.get("digits", 0.0)) * abs(spec.get("lsd", 0.0)) + abs(spec.get("abs", 0.0))
    return rect_to_std(limit) if spec.get("dist", "rect") == "rect" else limit


def uB_direct(value: pd.Series, spec: dict) -> pd.Series:
    v = pd.to_numeric(value, errors="coerce")
    limit = abs(spec.get("abs", 0.0)) + abs(spec.get("percent", 0.0)) * v.abs()
    u_acc = rect_to_std(limit) if spec.get("dist", "rect") == "rect" else limit
    u_res = res_to_std(abs(spec.get("resolution", 0.0)))
    return (u_acc**2 + u_res**2) ** 0.5


# -------------------------
# LabVIEW stats
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
    u_read = res_to_std(res_kg)
    u_delta = sqrt(2) * u_read
    out["uB_Consumo_kg_h"] = (u_delta / out["DeltaT_s"]) * 3600.0
    out.loc[out["DeltaT_s"] <= 0, "uB_Consumo_kg_h"] = pd.NA

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

    return out


def build_final_table(ponto: pd.DataFrame, lhv: pd.DataFrame, kibox_agg: pd.DataFrame, mappings: dict, instruments: dict) -> pd.DataFrame:
    df = ponto.merge(lhv, on=["EtOH_pct", "H2O_pct"], how="left")
    if kibox_agg is not None and not kibox_agg.empty:
        df = df.merge(kibox_agg, on=["Load_kW", "EtOH_pct", "H2O_pct"], how="left")

    P_mean = resolve_col(df, mappings["power_kw"]["mean"])
    F_mean = resolve_col(df, mappings["fuel_kgh"]["mean"])
    L_col = resolve_col(df, mappings["lhv_kj_kg"]["mean"])
    P_sd = resolve_col(df, mappings["power_kw"]["sd"])
    F_sd = resolve_col(df, mappings["fuel_kgh"]["sd"])
    N = pd.to_numeric(df["N_trechos_validos"], errors="coerce")

    df["uA_P_kw"] = pd.to_numeric(df[P_sd], errors="coerce") / (N**0.5)
    df["uA_Consumo_kg_h"] = pd.to_numeric(df[F_sd], errors="coerce") / (N**0.5)

    df["uB_P_kw"] = uB_power_kw(df[P_mean], instruments.get("power_kw", {}))
    df["uB_Consumo_kg_h"] = pd.to_numeric(df["uB_Consumo_kg_h_mean_of_windows"], errors="coerce")
    df["uB_LHV_kJ_kg"] = uB_direct(df[L_col], instruments.get("lhv_kj_kg", {}))

    df["uc_P_kw"] = (df["uA_P_kw"]**2 + df["uB_P_kw"]**2) ** 0.5
    df["U_P_kw"] = K_COVERAGE * df["uc_P_kw"]
    df["uc_Consumo_kg_h"] = (df["uA_Consumo_kg_h"]**2 + df["uB_Consumo_kg_h"]**2) ** 0.5
    df["U_Consumo_kg_h"] = K_COVERAGE * df["uc_Consumo_kg_h"]

    PkW = pd.to_numeric(df[P_mean], errors="coerce")
    Fkgh = pd.to_numeric(df[F_mean], errors="coerce")
    mdot = Fkgh / 3600.0
    LHVv = pd.to_numeric(df[L_col], errors="coerce")
    df["n_th"] = PkW / (mdot * LHVv)
    df["n_th_pct"] = df["n_th"] * 100.0

    rel_uc = ((df["uc_P_kw"] / PkW) ** 2 + (df["uc_Consumo_kg_h"] / Fkgh) ** 2 + (df["uB_LHV_kJ_kg"] / LHVv) ** 2) ** 0.5
    df["uc_n_th"] = df["n_th"] * rel_uc
    df["U_n_th"] = K_COVERAGE * df["uc_n_th"]
    df["U_n_th_pct"] = df["U_n_th"] * 100.0

    return df


# -------------------------
# PLOTS (somente _all) + pasta out/plots
# -------------------------
def plot_all_fuels(
    df: pd.DataFrame,
    y_col: str,
    yerr_col: Optional[str],
    title: str,
    filename: str,
    y_label: str,
    fixed_y: Optional[Tuple[float, float, int]] = None,
) -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    x_ticks = list(range(0, 56, 5))

    plt.figure()
    any_curve = False

    for h in FUEL_H2O_LEVELS:
        d = df[df["H2O_pct"].astype("Int64") == h].copy()
        d["Load_kW"] = pd.to_numeric(d["Load_kW"], errors="coerce")
        d[y_col] = pd.to_numeric(d[y_col], errors="coerce")
        if yerr_col:
            d[yerr_col] = pd.to_numeric(d[yerr_col], errors="coerce")
            d = d.dropna(subset=["Load_kW", y_col, yerr_col]).sort_values("Load_kW")
        else:
            d = d.dropna(subset=["Load_kW", y_col]).sort_values("Load_kW")

        if d.empty:
            continue

        any_curve = True
        if yerr_col:
            plt.errorbar(d["Load_kW"], d[y_col], yerr=d[yerr_col], fmt="o-", capsize=3, label=f"H2O={h}%")
        else:
            plt.plot(d["Load_kW"], d[y_col], "o-", label=f"H2O={h}%")

    if not any_curve:
        plt.close()
        print(f"[WARN] Sem dados para plot {filename}")
        return

    plt.xlim(0, 55)
    plt.xticks(x_ticks)
    if fixed_y is not None:
        ymin, ymax, step = fixed_y
        plt.ylim(ymin, ymax)
        plt.yticks(list(range(int(ymin), int(ymax) + 1, int(step))))
    plt.xlabel("Power (kW)")
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    plt.legend()
    outpath = PLOTS_DIR / filename
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()
    print(f"[OK] Salvei {outpath}")


def make_plots_all(out_df: pd.DataFrame) -> None:
    # n_th (fixo)
    if "n_th_pct" in out_df.columns and "U_n_th_pct" in out_df.columns:
        plot_all_fuels(
            out_df,
            y_col="n_th_pct",
            yerr_col="U_n_th_pct",
            title="n_th vs Power (all fuels)",
            filename="nth_vs_power_all.png",
            y_label="Thermal efficiency (%)",
            fixed_y=(0, 42, 2),
        )

    # consumo (com U expandida)
    cons_col = "Consumo_kg_h_mean_of_windows"
    if cons_col in out_df.columns and "U_Consumo_kg_h" in out_df.columns:
        plot_all_fuels(
            out_df,
            y_col=cons_col,
            yerr_col="U_Consumo_kg_h",
            title="Fuel consumption vs Power (all fuels)",
            filename="consumo_vs_power_all.png",
            y_label="Fuel consumption (kg/h)",
            fixed_y=None,
        )

    # todos Kibox params (plotar todos, sem SD)
    kibox_cols = [c for c in out_df.columns if str(c).startswith("KIBOX_")]
    # não faz sentido plotar contador de arquivos
    kibox_cols = [c for c in kibox_cols if c != "KIBOX_N_files"]
    for c in sorted(kibox_cols):
        safe = _safe_name(c)
        plot_all_fuels(
            out_df,
            y_col=c,
            yerr_col=None,
            title=f"{c} vs Power (all fuels)",
            filename=f"kibox_{safe}_vs_power_all.png",
            y_label=c,
            fixed_y=None,
        )


# -------------------------
# Passo 2: Histograma de knock (KPEAK_1) por combustível e carga
# -------------------------
def _find_kpeak_col(cols: List[str]) -> Optional[str]:
    # tenta nomes comuns (no CSV cru, geralmente não tem prefixo KIBOX_)
    preferred = ["KPEAK_1", "KPEAK1", "KPEAK_01", "KPEAK"]
    norm = {c: c.upper().replace(" ", "").replace("__", "_") for c in cols}

    # match direto
    for p in preferred:
        for c, cn in norm.items():
            if cn == p:
                return c

    # contains KPEAK
    for c, cn in norm.items():
        if "KPEAK" in cn and ("_1" in cn or cn.endswith("1")):
            return c

    # fuzzy
    close = difflib.get_close_matches("KPEAK_1", list(cols), n=3)
    return close[0] if close else None


def collect_kibox_kpeak_cycles(kibox_files: List[FileMeta]) -> pd.DataFrame:
    """
    Retorna dataframe com 1 linha por ciclo:
      Load_kW, EtOH_pct, H2O_pct, KPEAK
    """
    rows = []
    for m in kibox_files:
        if m.load_kw is None or m.etoh_pct is None or m.h2o_pct is None:
            continue
        try:
            df = read_kibox_csv_robust(m.path)
            kcol = _find_kpeak_col(list(df.columns))
            if not kcol:
                # não achou nesse arquivo
                continue
            k = _coerce_numeric_series(df[kcol])
            # elimina NaN
            k = k.dropna()
            if k.empty:
                continue
            rows.append(pd.DataFrame({
                "Load_kW": m.load_kw,
                "EtOH_pct": m.etoh_pct,
                "H2O_pct": m.h2o_pct,
                "KPEAK": k.values,
            }))
        except Exception as e:
            print(f"[ERROR] KPEAK parse falhou em {m.path.name}: {e}")

    if not rows:
        return pd.DataFrame(columns=["Load_kW", "EtOH_pct", "H2O_pct", "KPEAK"])

    return pd.concat(rows, ignore_index=True)


def plot_kpeak_histograms(kpeak_cycles: pd.DataFrame) -> None:
    """
    Um PNG com uma grade:
      - linhas = combustíveis (H2O 6/25/35)
      - colunas = cargas existentes
    bins: 0-0.5, 0.5-1, ..., 19.5-20, >20
    """
    if kpeak_cycles.empty:
        print("[WARN] Não gerei histograma KPEAK: sem dados.")
        return

    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    loads = sorted(pd.to_numeric(kpeak_cycles["Load_kW"], errors="coerce").dropna().unique().tolist())
    fuels = [h for h in FUEL_H2O_LEVELS if h in kpeak_cycles["H2O_pct"].astype("Int64").unique().tolist()]

    if not loads or not fuels:
        print("[WARN] Não gerei histograma KPEAK: faltam loads ou fuels.")
        return

    # bins
    edges = list(np.arange(0.0, 20.0 + 0.5, 0.5))  # 0..20 step 0.5
    # vamos tratar >20 separadamente
    labels = [f"{edges[i]:.1f}-{edges[i+1]:.1f}" for i in range(len(edges)-1)] + [">20"]

    nrows = len(fuels)
    ncols = len(loads)

    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(max(10, 3*ncols), max(6, 2.6*nrows)), sharey=True)

    # quando nrows/ncols = 1, axes não é matriz
    if nrows == 1 and ncols == 1:
        axes = np.array([[axes]])
    elif nrows == 1:
        axes = np.array([axes])
    elif ncols == 1:
        axes = np.array([[ax] for ax in axes])

    for r, h in enumerate(fuels):
        for c, L in enumerate(loads):
            ax = axes[r, c]
            d = kpeak_cycles[(kpeak_cycles["H2O_pct"].astype("Int64") == h) & (pd.to_numeric(kpeak_cycles["Load_kW"], errors="coerce") == L)]
            vals = pd.to_numeric(d["KPEAK"], errors="coerce").dropna().values

            if len(vals) == 0:
                ax.text(0.5, 0.5, "no data", ha="center", va="center", transform=ax.transAxes)
                ax.set_xticks([])
            else:
                # hist nos bins até 20
                inrange = vals[(vals >= 0) & (vals <= 20)]
                hist, _ = np.histogram(inrange, bins=edges)
                gt20 = np.sum(vals > 20)
                counts = np.concatenate([hist, np.array([gt20])])

                ax.bar(range(len(counts)), counts)
                ax.set_xticks([0, 10, 20, 30, 40])  # poucos ticks (senão fica ilegível)
                ax.set_xticklabels(["0", "5", "10", "15", ">20"])

            if r == 0:
                ax.set_title(f"{int(L)} kW")
            if c == 0:
                ax.set_ylabel(f"H2O={h}%\ncount")

            ax.grid(True, axis="y", linestyle="--", linewidth=0.5)

    fig.suptitle("KPEAK distribution per cycle (bins: 0–20 step 0.5, and >20)", fontsize=12)
    fig.text(0.5, 0.04, "KPEAK bin (approx. bar)", ha="center")

    outpath = PLOTS_DIR / "kibox_KPEAK_histograms_all_fuels.png"
    plt.tight_layout(rect=[0, 0.06, 1, 0.93])
    plt.savefig(outpath, dpi=220)
    plt.close()
    print(f"[OK] Salvei {outpath}")


# -------------------------
# main
# -------------------------
def main() -> None:
    OUT_DIR.mkdir(exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    raw_files = [p for p in RAW_DIR.glob("*") if p.is_file() and not p.name.startswith("~$")]
    metas = [parse_meta(p) for p in raw_files]

    lv_files = [m for m in metas if m.source_type == "LABVIEW" and m.path.suffix.lower() == ".xlsx"]
    kibox_files = [m for m in metas if m.source_type == "KIBOX" and m.path.suffix.lower() == ".csv"]

    if not lv_files:
        raise SystemExit("Não achei .xlsx do LabVIEW em raw/.")

    mappings, instruments = load_config_excel()

    lv_all: List[pd.DataFrame] = []
    for m in lv_files:
        try:
            lv_all.append(read_labview_xlsx(m))
        except Exception as e:
            print(f"[ERROR] LabVIEW {m.path.name}: {e}")
    lv_all = [d for d in lv_all if not d.empty]
    if not lv_all:
        raise SystemExit("Nenhum arquivo LabVIEW foi lido com sucesso.")

    lv_raw = pd.concat(lv_all, ignore_index=True)
    trechos = compute_trechos_stats(lv_raw, instruments)
    ponto = compute_ponto_stats(trechos)

    lhv = load_lhv_lookup()
    kibox_agg = kibox_aggregate(kibox_files) if kibox_files else pd.DataFrame(columns=["Load_kW", "EtOH_pct", "H2O_pct"])

    out = build_final_table(ponto, lhv, kibox_agg, mappings, instruments)
    out_xlsx = safe_to_excel(out, OUT_DIR / "lv_kpis_clean.xlsx")
    print(f"[OK] Excel gerado: {out_xlsx}")

    # plots (todos em out/plots)
    make_plots_all(out)

    # passo 2: histogramas KPEAK
    if kibox_files:
        kpeak_cycles = collect_kibox_kpeak_cycles(kibox_files)
        # loga qual coluna pegou (se quiser depois eu mostro)
        plot_kpeak_histograms(kpeak_cycles)
    else:
        print("[WARN] Sem arquivos Kibox; não gerei histograma KPEAK.")


if __name__ == "__main__":
    main()