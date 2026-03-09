from __future__ import annotations

import csv
import re
import shutil
import unicodedata
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
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_RAW_DIR = BASE_DIR / "raw"
DEFAULT_PROCESS_DIR = DEFAULT_RAW_DIR / "PROCESSAR"
DEFAULT_OUT_DIR = BASE_DIR / "out"
MESTRADO_ROOT = Path(r"D:\Drive\Faculdade\PUC\Mestrado")

RAW_DIR = DEFAULT_RAW_DIR
PROCESS_DIR = DEFAULT_PROCESS_DIR
OUT_DIR = DEFAULT_OUT_DIR
PLOTS_DIR = OUT_DIR / "plots"
CFG_DIR = BASE_DIR / "config"

PREFERRED_SHEET_NAME = "labview"
B_ETANOL_COL_CANDIDATES = ["B_Etanol", "B_ETANOL", "B_ETANOL (kg)", "B_Etanol (kg)"]

SAMPLES_PER_WINDOW = 30
MIN_SAMPLES_PER_WINDOW = 30
DT_S = 1.0
TIME_DELTA_ERROR_THRESHOLD_S = 1.2
TIME_DELTA_PLOT_YMIN_S = 0.8
TIME_DELTA_PLOT_YMAX_S = 1.6
TIME_DELTA_PLOT_YSTEP_S = 0.1
DEFAULT_MAX_DELTA_BETWEEN_SAMPLES_MS = 1200.0
DEFAULT_MAX_ACT_CONTROL_ERROR_C = 5.0
DEFAULT_MAX_ECT_CONTROL_ERROR_C = 2.0
K_COVERAGE = 2.0


def _path_is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False


def is_mestrado_runtime() -> bool:
    return _path_is_within(Path.cwd(), MESTRADO_ROOT)

FUEL_H2O_LEVELS = [6, 25, 35]  # â€œcombustÃ­veisâ€ por hidrataÃ§Ã£o
COMPOSITION_COLS = ["DIES_pct", "BIOD_pct", "EtOH_pct", "H2O_pct"]

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


def _canon_name(x: object) -> str:
    s = str(x).replace("\ufeff", "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", s)


def _normalize_repeated_stat_tokens_in_name(x: object) -> str:
    s = str(x).replace("\ufeff", "").strip()
    if not s:
        return s

    replacements = [
        ("_mean_mean_of_windows", "_mean_of_windows"),
        ("_mean_sd_of_windows", "_sd_of_windows"),
        ("_sd_mean_of_windows", "_sd_of_windows"),
        ("_sd_sd_of_windows", "_sd_of_windows"),
        ("_mean_mean", "_mean"),
        ("_sd_sd", "_sd"),
    ]

    prev = None
    while prev != s:
        prev = s
        for old, new in replacements:
            s = s.replace(old, new)
    s = re.sub(r"__+", "_", s)
    return s


def _coalesce_equivalent_columns(df: pd.DataFrame, context: str = "") -> pd.DataFrame:
    if df is None or df.empty:
        return df

    merged: Dict[str, pd.Series] = {}
    sources: Dict[str, List[str]] = {}
    for idx, raw_col in enumerate(df.columns):
        col = _normalize_repeated_stat_tokens_in_name(raw_col)
        series = df.iloc[:, idx].copy()
        series.name = col
        sources.setdefault(col, []).append(str(raw_col))
        if col in merged:
            merged[col] = merged[col].where(merged[col].notna(), series)
        else:
            merged[col] = series

    duplicates = {col: cols for col, cols in sources.items() if len(cols) > 1}
    if duplicates:
        preview = "; ".join(f"{col} <= {cols}" for col, cols in list(duplicates.items())[:5])
        where = f" em {context}" if context else ""
        print(f"[INFO] Consolidei colunas equivalentes{where}: {preview}")

    return pd.DataFrame(merged, index=df.index)


def resolve_col(df: pd.DataFrame, requested: str) -> str:
    requested = str(requested).replace("\ufeff", "").strip()
    if not requested:
        raise KeyError("Nome de coluna solicitado estÃ¡ vazio (verifique Mappings no config).")

    if requested in df.columns:
        return requested

    low_map = {str(c).lower().strip(): c for c in df.columns}
    req_low = requested.lower().strip()
    if req_low in low_map:
        return low_map[req_low]

    canon_map = {_canon_name(c): c for c in df.columns}
    req_canon = _canon_name(requested)
    if req_canon in canon_map:
        return canon_map[req_canon]

    req_stats_norm = _normalize_repeated_stat_tokens_in_name(requested)
    if req_stats_norm in df.columns:
        return req_stats_norm

    stats_norm_map: Dict[str, str] = {}
    for c in df.columns:
        c_norm = _normalize_repeated_stat_tokens_in_name(c)
        if c_norm not in stats_norm_map:
            stats_norm_map[c_norm] = c
    if req_stats_norm in stats_norm_map:
        return stats_norm_map[req_stats_norm]

    stats_norm_canon_map: Dict[str, str] = {}
    for c in df.columns:
        c_norm = _normalize_repeated_stat_tokens_in_name(c)
        c_norm_canon = _canon_name(c_norm)
        if c_norm_canon not in stats_norm_canon_map:
            stats_norm_canon_map[c_norm_canon] = c
    req_stats_canon = _canon_name(req_stats_norm)
    if req_stats_canon in stats_norm_canon_map:
        return stats_norm_canon_map[req_stats_canon]

    suggestion = difflib.get_close_matches(requested, list(df.columns), n=6)
    sug_txt = f" SugestÃµes: {suggestion}" if suggestion else ""
    raise KeyError(f"Coluna '{requested}' nÃ£o encontrada no dataframe.{sug_txt}")


def safe_to_excel(df: pd.DataFrame, path: Path) -> Path:
    try:
        df.to_excel(path, index=False)
        return path
    except PermissionError:
        ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        alt = path.with_name(f"{path.stem}_{ts}{path.suffix}")
        df.to_excel(alt, index=False)
        return alt


def clear_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for child in path.iterdir():
        if child.is_dir():
            clear_output_dir(child)
            try:
                child.rmdir()
            except OSError:
                pass
            continue
        try:
            child.unlink()
        except PermissionError as e:
            raise SystemExit(
                f"NÃ£o consegui limpar o output porque '{child}' estÃ¡ em uso. "
                "Feche o arquivo ou o programa que o estÃ¡ usando e rode novamente."
            ) from e


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


def _basename_parts(basename: object) -> List[str]:
    return [str(p).strip() for p in str(basename).split("__") if str(p).strip()]


def _basename_source_folder_parts(basename: object) -> List[str]:
    parts = _basename_parts(basename)
    if len(parts) <= 1:
        return []
    return parts[:-1]


def _basename_source_folder_display(basename: object) -> str:
    return " / ".join(_basename_source_folder_parts(basename))


def _basename_source_file(basename: object) -> str:
    parts = _basename_parts(basename)
    if not parts:
        return ""
    return parts[-1]


def _basename_source_plot_dir(basename: object, root: Path | None = None) -> Path:
    base_root = PLOTS_DIR if root is None else root
    folder_parts = _basename_source_folder_parts(basename)
    if not folder_parts:
        return base_root
    return base_root.joinpath(*folder_parts)


def add_source_identity_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "BaseName" not in df.columns:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()

    out = df.copy()
    out["SourceFolder"] = out["BaseName"].map(_basename_source_folder_display)
    out["SourceFile"] = out["BaseName"].map(_basename_source_file)
    return out


def iter_source_plot_groups(df: pd.DataFrame, root: Path | None = None) -> List[Tuple[str, Path, pd.DataFrame]]:
    if df is None or df.empty:
        return []

    if "BaseName" not in df.columns:
        base_root = PLOTS_DIR if root is None else root
        return [("", base_root, df.copy())]

    tmp = add_source_identity_columns(df)
    groups: List[Tuple[str, Path, pd.DataFrame]] = []
    for source_folder, d in tmp.groupby("SourceFolder", dropna=False, sort=True):
        basename_example = d["BaseName"].iloc[0]
        plot_dir = _basename_source_plot_dir(basename_example, root=root)
        groups.append((str(source_folder or ""), plot_dir, d.copy()))
    return groups


def _source_folder_leaf(source_folder: object) -> str:
    s = str(source_folder or "").strip()
    if not s:
        return ""
    parts = [p.strip() for p in s.split("/") if p.strip()]
    return parts[-1] if parts else s


def _normalize_compare_series_name(source_folder: object) -> str:
    leaf = _source_folder_leaf(source_folder)
    if not leaf:
        return "origem_desconhecida"

    s = _canon_name(leaf).replace(" ", "_").replace("-", "_")
    s = re.sub(r"_+", "_", s).strip("_")
    s = re.sub(r"(^|_)subindo(?=_|$)", r"\1subida", s)
    s = re.sub(r"(^|_)descendo(?=_|$)", r"\1descida", s)
    if not s:
        return "origem_desconhecida"
    return s


def _safe_folder_name(name: object) -> str:
    s = str(name or "").strip()
    if not s:
        return "compare"
    s = re.sub(r'[<>:"/\\|?*]', "_", s)
    s = s.strip().rstrip(".")
    return s if s else "compare"


def _infer_source_direction_from_folder_name(source_folder: object) -> Optional[str]:
    s = _canon_name(source_folder).replace("_", " ").replace("-", " ")
    if "subindo" in s or "subida" in s or re.search(r"\bup\b", s):
        return "subindo"
    if "descendo" in s or "descida" in s or re.search(r"\bdown\b", s):
        return "descendo"
    return None


def _compare_group_key_from_source_folder(source_folder: object) -> str:
    s = str(source_folder or "").strip()
    if not s:
        return ""

    parts = [p.strip() for p in s.split("/") if p.strip()]
    clean_parts: List[str] = []
    for part in parts:
        t = _canon_name(part).replace("_", " ").replace("-", " ")
        t = re.sub(r"\b(subindo|subida|descendo|descida|up|down)\b", " ", t)
        t = re.sub(r"\s+", " ", t).strip()
        if t:
            clean_parts.append(t)

    if not clean_parts:
        return ""
    return "__".join(_safe_name(p) for p in clean_parts)


def iter_compare_plot_groups(df: pd.DataFrame, root: Path | None = None) -> List[Tuple[str, Path, pd.DataFrame]]:
    """
    Build compare groups combining subida/descida datasets for the same run key.
    Output path pattern: <root>/compare/<group_key>/...
    """
    if df is None or df.empty:
        return []

    tmp = add_source_identity_columns(df)
    if "SourceFolder" not in tmp.columns:
        return []

    tmp = tmp.copy()
    tmp["_COMPARE_GROUP"] = tmp["SourceFolder"].map(_compare_group_key_from_source_folder)
    tmp["_COMPARE_SERIES"] = tmp["SourceFolder"].map(_normalize_compare_series_name)
    tmp["_COMPARE_DIRECTION"] = tmp["SourceFolder"].map(_infer_source_direction_from_folder_name)
    tmp["_COMPARE_SERIES"] = tmp["_COMPARE_SERIES"].where(
        tmp["_COMPARE_SERIES"].map(lambda x: not _is_blank_cell(x)),
        "origem_desconhecida",
    )

    base_root = (PLOTS_DIR if root is None else root) / "compare"
    groups: List[Tuple[str, Path, pd.DataFrame]] = []
    for group_key, d in tmp.groupby("_COMPARE_GROUP", dropna=False, sort=True):
        gk = str(group_key or "").strip()
        if not gk:
            continue

        dirs = set(str(x).strip().lower() for x in d["_COMPARE_DIRECTION"].dropna().tolist() if str(x).strip())
        # Compare plots are only useful when both directions exist.
        if "subindo" not in dirs or "descendo" not in dirs:
            continue

        subida_vals = sorted(
            set(
                str(v).strip()
                for v in d.loc[d["_COMPARE_DIRECTION"].eq("subindo"), "_COMPARE_SERIES"].dropna().tolist()
                if str(v).strip()
            )
        )
        descida_vals = sorted(
            set(
                str(v).strip()
                for v in d.loc[d["_COMPARE_DIRECTION"].eq("descendo"), "_COMPARE_SERIES"].dropna().tolist()
                if str(v).strip()
            )
        )
        if subida_vals and descida_vals:
            compare_name = f"{subida_vals[0]} vs {descida_vals[0]}"
        else:
            uniq = sorted(
                set(str(v).strip() for v in d["_COMPARE_SERIES"].dropna().tolist() if str(v).strip())
            )
            compare_name = " vs ".join(uniq[:2]) if uniq else gk

        plot_dir = base_root / _safe_folder_name(compare_name)
        groups.append((gk, plot_dir, d.copy()))

    return groups


def _infer_sentido_carga_from_folder_parts(parts: List[str]) -> object:
    for part in reversed(parts):
        s = _canon_name(part).replace("_", " ").replace("-", " ")
        if "subindo" in s or "subida" in s or re.search(r"\bup\b", s):
            return "subida"
        if "descendo" in s or "descida" in s or re.search(r"\bdown\b", s):
            return "descida"
    return pd.NA


def _infer_iteracao_from_folder_parts(parts: List[str]) -> object:
    for part in reversed(parts):
        m = re.search(r"(\d+)\s*$", str(part))
        if m:
            return int(m.group(1))

    for part in reversed(parts):
        nums = re.findall(r"\d+", str(part))
        if nums:
            return int(nums[-1])

    return pd.NA


def _sentido_carga_rank(x: object) -> int:
    s = _canon_name(x).replace("_", " ").replace("-", " ")
    if "subida" in s or "subindo" in s or re.search(r"\bup\b", s):
        return 0
    if "descida" in s or "descendo" in s or re.search(r"\bdown\b", s):
        return 1
    return 9


def add_run_context_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "BaseName" not in df.columns:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()

    out = df.copy()
    folder_parts = out["BaseName"].map(_basename_source_folder_parts)
    out["Sentido_Carga"] = folder_parts.map(_infer_sentido_carga_from_folder_parts)
    out["Iteracao"] = pd.to_numeric(folder_parts.map(_infer_iteracao_from_folder_parts), errors="coerce").astype("Int64")
    return out


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
        print("[WARN] Airflow: nÃ£o achei coluna de consumo (kg/h). Pulando canais de ar.")
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
    source_type: str  # "LABVIEW" or "KIBOX" or "MOTEC"
    load_kw: Optional[float]
    dies_pct: Optional[float]
    biod_pct: Optional[float]
    etoh_pct: Optional[int]
    h2o_pct: Optional[int]
    load_parse: str = ""
    composition_parse: str = ""


def _to_pct_or_none(x: object) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(str(x).replace(",", "."))
    except Exception:
        return None
    if not np.isfinite(v):
        return None
    return v


def _parse_filename_composition(stem: str) -> Tuple[Optional[float], Optional[float], Optional[int], Optional[int], str]:
    m_eh = re.search(r"E(\d+)\s*H(\d+)", stem, flags=re.IGNORECASE)
    if m_eh:
        return None, None, int(m_eh.group(1)), int(m_eh.group(2)), "filename_ethanol"

    dies_pct = None
    biod_pct = None

    m_db = re.search(r"(?:^|[^A-Za-z0-9])D(\d+(?:[.,]\d+)?)\s*B(\d+(?:[.,]\d+)?)(?:$|[^A-Za-z0-9])", stem, flags=re.IGNORECASE)
    if m_db:
        dies_pct = _to_pct_or_none(m_db.group(1))
        biod_pct = _to_pct_or_none(m_db.group(2))
        return dies_pct, biod_pct, None, None, "filename_diesel"

    m_dies = re.search(r"(?:dies_pct|diesel|dies)\s*[-_ ]*(\d+(?:[.,]\d+)?)", stem, flags=re.IGNORECASE)
    if m_dies:
        dies_pct = _to_pct_or_none(m_dies.group(1))

    m_biod = re.search(r"(?:biod_pct|biodiesel|biod)\s*[-_ ]*(\d+(?:[.,]\d+)?)", stem, flags=re.IGNORECASE)
    if m_biod:
        biod_pct = _to_pct_or_none(m_biod.group(1))

    if dies_pct is None and biod_pct is not None and 0.0 <= biod_pct <= 100.0:
        dies_pct = 100.0 - biod_pct
        return dies_pct, biod_pct, None, None, "filename_diesel_inferred"

    if biod_pct is None and dies_pct is not None and 0.0 <= dies_pct <= 100.0:
        biod_pct = 100.0 - dies_pct
        return dies_pct, biod_pct, None, None, "filename_diesel_inferred"

    if dies_pct is not None or biod_pct is not None:
        return dies_pct, biod_pct, None, None, "filename_diesel"

    return None, None, None, None, "missing_filename"


def parse_meta(path: Path) -> FileMeta:
    try:
        rel = path.relative_to(PROCESS_DIR)
        basename = "__".join(rel.with_suffix("").parts)
    except Exception:
        try:
            rel = path.relative_to(RAW_DIR)
            basename = "__".join(rel.with_suffix("").parts)
        except Exception:
            basename = "__".join((path.parent.name, path.stem))

    stem_lower = path.stem.lower()
    if stem_lower.endswith("_i"):
        source_type = "KIBOX"
    elif stem_lower.endswith("_m"):
        source_type = "MOTEC"
    else:
        source_type = "LABVIEW"

    load_tokens = re.findall(r"(\d+(?:[.,]\d+)?)\s*[-_ ]?\s*kw", path.stem, flags=re.IGNORECASE)
    if not load_tokens:
        bare_num = re.fullmatch(r"\s*(\d+(?:[.,]\d+)?)\s*", path.stem)
        if bare_num:
            load_tokens = [bare_num.group(1)]

    load_candidates: List[float] = []
    for tok in load_tokens:
        val = float(str(tok).replace(",", "."))
        if val not in load_candidates:
            load_candidates.append(val)

    if len(load_candidates) == 1:
        load_kw = load_candidates[0]
        load_parse = "filename"
    elif len(load_candidates) > 1:
        load_kw = None
        load_parse = "ambiguous_filename"
    else:
        load_kw = None
        load_parse = "missing_filename"

    dies_pct, biod_pct, etoh_pct, h2o_pct, composition_parse = _parse_filename_composition(path.stem)

    return FileMeta(
        path=path,
        basename=basename,
        source_type=source_type,
        load_kw=load_kw,
        dies_pct=dies_pct,
        biod_pct=biod_pct,
        etoh_pct=etoh_pct,
        h2o_pct=h2o_pct,
        load_parse=load_parse,
        composition_parse=composition_parse,
    )


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

    raise ValueError(f"NÃ£o encontrei aba '{PREFERRED_SHEET_NAME}' e existem mÃºltiplas abas em {path.name}: {sheets}.")


def find_b_etanol_col(df: pd.DataFrame) -> str:
    for c in B_ETANOL_COL_CANDIDATES:
        if c in df.columns:
            return c
    raise KeyError(
        f"NÃ£o encontrei coluna de balanÃ§a. Procurei: {B_ETANOL_COL_CANDIDATES}. "
        f"Colunas (primeiras 40): {list(df.columns)[:40]}"
    )


def _infer_load_series_from_signal(df: pd.DataFrame) -> Optional[pd.Series]:
    load_col = "Carga (kW)" if "Carga (kW)" in df.columns else _find_first_col_by_substrings(df, ["carga", "kw"])
    if not load_col:
        return None

    v = pd.to_numeric(df[load_col], errors="coerce")
    if v.notna().sum() == 0:
        return None

    # Quantize the measured load to the expected 0.5 kW steps to keep grouping stable.
    return pd.Series(np.round(v * 2.0) / 2.0, index=df.index)


def _infer_single_load_from_signal(df: pd.DataFrame) -> Optional[float]:
    inferred = _infer_load_series_from_signal(df)
    if inferred is None:
        return None

    vals = sorted(pd.unique(pd.to_numeric(inferred, errors="coerce").dropna()))
    if len(vals) != 1:
        return None
    return float(vals[0])


def read_labview_xlsx(meta: FileMeta) -> pd.DataFrame:
    sheet = choose_labview_sheet(meta.path)
    df = _read_excel(meta.path, sheet_name=sheet)

    df.columns = _normalize_cols(list(df.columns))
    df = df.loc[:, ~pd.Series(df.columns).astype(str).str.startswith("Unnamed").values].copy()

    df = df.reset_index(drop=True)
    df["Index"] = range(len(df))
    df["WindowID"] = df["Index"] // SAMPLES_PER_WINDOW

    load_series = pd.Series(meta.load_kw, index=df.index, dtype="float64")
    load_signal_series = pd.Series(np.nan, index=df.index, dtype="float64")
    dies_series = pd.Series(meta.dies_pct, index=df.index, dtype="float64")
    biod_series = pd.Series(meta.biod_pct, index=df.index, dtype="float64")
    etoh_series = pd.Series(meta.etoh_pct, index=df.index, dtype="float64")
    h2o_series = pd.Series(meta.h2o_pct, index=df.index, dtype="float64")
    inferred_load = _infer_load_series_from_signal(df)
    inferred_single = _infer_single_load_from_signal(df)

    if inferred_load is not None and inferred_load.notna().any():
        load_signal_series = pd.to_numeric(inferred_load, errors="coerce")

    if meta.load_kw is None or meta.load_parse == "ambiguous_filename":
        if inferred_load is not None and inferred_load.notna().any():
            load_series = pd.to_numeric(inferred_load, errors="coerce")
            print(f"[INFO] Load_kW inferido pela coluna de carga para {meta.basename} ({meta.load_parse}).")
        elif meta.load_kw is None:
            print(f"[WARN] Load_kW nÃ£o identificado para {meta.basename}; a coluna ficarÃ¡ vazia no output.")
    elif inferred_single is not None and abs(inferred_single - float(meta.load_kw)) > 0.75:
        print(
            f"[WARN] Load_kW do nome ({meta.load_kw:g}) difere da carga medida ({inferred_single:g}) "
            f"em {meta.basename}. Vou preservar o load nominal do nome e guardar a carga inferida em Load_Signal_kW."
        )

    df = df.assign(
        BaseName=meta.basename,
        Load_kW=load_series,
        Load_Signal_kW=load_signal_series,
        DIES_pct=dies_series,
        BIOD_pct=biod_series,
        EtOH_pct=etoh_series,
        H2O_pct=h2o_series,
    )

    first_cols = ["BaseName", "Load_kW", "Load_Signal_kW", "DIES_pct", "BIOD_pct", "EtOH_pct", "H2O_pct", "Index", "WindowID"]
    rest = [c for c in df.columns if c not in first_cols]
    return df[first_cols + rest].copy()


def _read_motec_metadata(path: Path, delim: str = ",") -> Dict[str, float]:
    meta: Dict[str, float] = {}
    with path.open("r", encoding="latin-1", errors="ignore", newline="") as fh:
        reader = csv.reader(fh, delimiter=delim)
        for i, row in enumerate(reader, start=1):
            if i > 14:
                break
            if not row:
                continue
            key = str(row[0]).replace("\ufeff", "").strip().strip('"')
            key_norm = norm_key(key)
            if key_norm == "sample rate" and len(row) > 1:
                meta["Motec_SampleRate_Hz"] = _to_float(row[1], default=np.nan)
            elif key_norm == "duration" and len(row) > 1:
                meta["Motec_Duration_s"] = _to_float(row[1], default=np.nan)
    return meta


def read_motec_csv(meta: FileMeta) -> pd.DataFrame:
    text = meta.path.read_text(encoding="latin-1", errors="ignore")
    sample = "\n".join(text.splitlines()[:20])
    delim = _sniff_delimiter(sample)

    try:
        df = pd.read_csv(meta.path, sep=delim, engine="python", encoding="utf-8-sig", skiprows=14)
    except UnicodeDecodeError:
        df = pd.read_csv(meta.path, sep=delim, engine="python", encoding="latin-1", skiprows=14)
    df.columns = _normalize_cols(list(df.columns))
    df = df.loc[:, ~pd.Series(df.columns).astype(str).str.startswith("Unnamed").values].copy()
    if len(df) < 1:
        raise ValueError(f"Arquivo MOTEC sem linhas de dados apos o cabecalho: {meta.path.name}")

    # Row 16 in the source file contains units. Data starts on row 17.
    df = df.iloc[1:].reset_index(drop=True).copy()
    motec_cols = []
    for i, c in enumerate(df.columns):
        clean = str(c).replace("\ufeff", "").strip()
        if not clean:
            clean = f"Col_{i + 1}"
        motec_cols.append(f"Motec_{clean}")
    df.columns = motec_cols

    meta_cols = _read_motec_metadata(meta.path, delim=delim)
    for key, value in meta_cols.items():
        df[key] = value

    time_col = next((c for c in df.columns if norm_key(c) == norm_key("Motec_Time")), "")
    if time_col:
        t = pd.to_numeric(df[time_col], errors="coerce")
        df["Motec_Time_Delta_s"] = t.diff()

    df = df.reset_index(drop=True)
    df["Index"] = range(len(df))
    df["WindowID"] = df["Index"] // SAMPLES_PER_WINDOW
    df["BaseName"] = meta.basename
    df["Load_kW"] = pd.Series(meta.load_kw, index=df.index, dtype="float64")
    df["DIES_pct"] = pd.Series(meta.dies_pct, index=df.index, dtype="float64")
    df["BIOD_pct"] = pd.Series(meta.biod_pct, index=df.index, dtype="float64")
    df["EtOH_pct"] = pd.Series(meta.etoh_pct, index=df.index, dtype="float64")
    df["H2O_pct"] = pd.Series(meta.h2o_pct, index=df.index, dtype="float64")

    first_cols = ["BaseName", "Load_kW", "DIES_pct", "BIOD_pct", "EtOH_pct", "H2O_pct", "Index", "WindowID"]
    rest = [c for c in df.columns if c not in first_cols]
    return df[first_cols + rest].copy()


def _parse_time_series(s: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s, errors="coerce")

    dt = pd.to_datetime(s, errors="coerce")
    if dt.notna().any():
        return dt

    v = pd.to_numeric(s, errors="coerce")
    if v.notna().sum() == 0:
        return pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

    # Fallback for Excel serial date/time values.
    return pd.to_datetime(v, unit="D", origin="1899-12-30", errors="coerce")


def build_time_diagnostics(
    lv_raw: pd.DataFrame,
    time_col: str = "Time",
    quality_cfg: Optional[Dict[str, float]] = None,
) -> pd.DataFrame:
    quality_cfg = quality_cfg or {}
    max_delta_ms = _to_float(
        quality_cfg.get("MAX_DELTA_BETWEEN_SAMPLES_ms", DEFAULT_MAX_DELTA_BETWEEN_SAMPLES_MS),
        DEFAULT_MAX_DELTA_BETWEEN_SAMPLES_MS,
    )
    max_delta_s = max_delta_ms / 1000.0
    max_act_error_c = _to_float(
        quality_cfg.get("MAX_ACT_CONTROL_ERROR", DEFAULT_MAX_ACT_CONTROL_ERROR_C),
        DEFAULT_MAX_ACT_CONTROL_ERROR_C,
    )
    max_ect_error_c = _to_float(
        quality_cfg.get("MAX_ECT_CONTROL_ERROR", DEFAULT_MAX_ECT_CONTROL_ERROR_C),
        DEFAULT_MAX_ECT_CONTROL_ERROR_C,
    )

    try:
        time_col = resolve_col(lv_raw, time_col)
    except Exception:
        try:
            time_col = resolve_col(lv_raw, "TIME")
        except Exception:
            return pd.DataFrame()

    if time_col not in lv_raw.columns:
        return pd.DataFrame()

    base_cols = [c for c in ["BaseName", "Load_kW", "DIES_pct", "BIOD_pct", "EtOH_pct", "H2O_pct", "Index"] if c in lv_raw.columns]
    out = lv_raw[base_cols + [time_col]].copy()
    out = add_source_identity_columns(out)
    out = add_run_context_columns(out)

    t = _parse_time_series(out[time_col])
    out["TIME_PARSED"] = t
    out["TIME_TEXT_ms"] = t.dt.strftime("%Y-%m-%d %H:%M:%S.%f").str[:-3]
    out["TIME_HOUR"] = t.dt.hour.astype("Int64")
    out["TIME_MINUTE"] = t.dt.minute.astype("Int64")
    out["TIME_SECOND"] = t.dt.second.astype("Int64")
    out["TIME_MILLISECOND"] = (t.dt.microsecond // 1000).astype("Int64")

    prev_t = t.groupby(out["BaseName"], dropna=False).shift(1)
    next_t = t.groupby(out["BaseName"], dropna=False).shift(-1)

    out["TIME_DELTA_FROM_PREV_s"] = (t - prev_t).dt.total_seconds()
    out["TIME_DELTA_TO_NEXT_s"] = (next_t - t).dt.total_seconds()
    out["TIME_DELTA_TO_NEXT_ms"] = pd.to_numeric(out["TIME_DELTA_TO_NEXT_s"], errors="coerce") * 1000.0

    typical_dt = out.groupby("BaseName", dropna=False)["TIME_DELTA_TO_NEXT_s"].transform(
        lambda s: float(pd.to_numeric(s, errors="coerce").dropna().median()) if pd.to_numeric(s, errors="coerce").dropna().any() else np.nan
    )
    out["TIME_DELTA_REFERENCE_s"] = typical_dt
    out["TIME_DELTA_ERROR_ms"] = (pd.to_numeric(out["TIME_DELTA_TO_NEXT_s"], errors="coerce") - pd.to_numeric(typical_dt, errors="coerce")) * 1000.0
    out["MAX_DELTA_BETWEEN_SAMPLES_ms"] = max_delta_ms
    out["TIME_DELTA_LIMIT_s"] = max_delta_s
    out["TIME_DELTA_LIMIT_ms"] = max_delta_ms
    out["TIME_DELTA_ERROR_FLAG"] = pd.to_numeric(out["TIME_DELTA_TO_NEXT_s"], errors="coerce") > max_delta_s
    out["TIME_SAMPLE_GLOBAL"] = np.arange(len(out), dtype=int)

    t_adm_col = "T_ADMISSAO" if "T_ADMISSAO" in lv_raw.columns else _find_first_col_by_substrings(lv_raw, ["t", "admiss"])
    dem_act_col = (
        "DEM ACT AQUECEDOR"
        if "DEM ACT AQUECEDOR" in lv_raw.columns
        else _find_first_col_by_substrings(lv_raw, ["dem", "act"])
    )
    out["MAX_ACT_CONTROL_ERROR"] = max_act_error_c
    out["ACT_CTRL_ACTUAL_C"] = pd.NA
    out["ACT_CTRL_TARGET_C"] = pd.NA
    out["ACT_CTRL_ERROR_C"] = pd.NA
    out["ACT_CTRL_ERROR_ABS_C"] = pd.NA
    out["ACT_CTRL_ERROR_FLAG"] = pd.NA
    if t_adm_col and dem_act_col:
        act_actual = pd.to_numeric(lv_raw[t_adm_col], errors="coerce")
        act_target = pd.to_numeric(lv_raw[dem_act_col], errors="coerce")
        act_err = act_actual - act_target
        out["ACT_CTRL_ACTUAL_C"] = act_actual
        out["ACT_CTRL_TARGET_C"] = act_target
        out["ACT_CTRL_ERROR_C"] = act_err
        out["ACT_CTRL_ERROR_ABS_C"] = act_err.abs()
        out["ACT_CTRL_ERROR_FLAG"] = act_err.abs() > max_act_error_c

    t_s_agua_col = None
    for cand in ["T_S_AGUA", "T_S_ÃGUA", "T_S AGUA", "T_S ÃGUA"]:
        if cand in lv_raw.columns:
            t_s_agua_col = cand
            break
    if t_s_agua_col is None:
        t_s_agua_col = _find_first_col_by_substrings(lv_raw, ["t_s", "agua"])
    if t_s_agua_col is None:
        t_s_agua_col = _find_first_col_by_substrings(lv_raw, ["t_s", "Ã¡gua"])

    dem_th2o_col = None
    for cand in ["DEM_TH2O", "DEM TH2O"]:
        if cand in lv_raw.columns:
            dem_th2o_col = cand
            break
    if dem_th2o_col is None:
        dem_th2o_col = _find_first_col_by_substrings(lv_raw, ["dem", "th2o"])

    out["MAX_ECT_CONTROL_ERROR"] = max_ect_error_c
    out["ECT_CTRL_ACTUAL_C"] = pd.NA
    out["ECT_CTRL_TARGET_C"] = pd.NA
    out["ECT_CTRL_LIMIT_LOW_C"] = pd.NA
    out["ECT_CTRL_LIMIT_HIGH_C"] = pd.NA
    out["ECT_CTRL_ERROR_C"] = pd.NA
    out["ECT_CTRL_ERROR_ABS_C"] = pd.NA
    out["ECT_CTRL_ERROR_FLAG"] = pd.NA
    if t_s_agua_col and dem_th2o_col:
        ect_actual = pd.to_numeric(lv_raw[t_s_agua_col], errors="coerce")
        ect_target = pd.to_numeric(lv_raw[dem_th2o_col], errors="coerce")
        ect_err = ect_actual - ect_target
        out["ECT_CTRL_ACTUAL_C"] = ect_actual
        out["ECT_CTRL_TARGET_C"] = ect_target
        out["ECT_CTRL_LIMIT_LOW_C"] = ect_target - max_ect_error_c
        out["ECT_CTRL_LIMIT_HIGH_C"] = ect_target + max_ect_error_c
        out["ECT_CTRL_ERROR_C"] = ect_err
        out["ECT_CTRL_ERROR_ABS_C"] = ect_err.abs()
        out["ECT_CTRL_ERROR_FLAG"] = ect_err.abs() > max_ect_error_c

    return out


def _time_diag_load_title(load_kw: object) -> str:
    v = pd.to_numeric(pd.Series([load_kw]), errors="coerce").iloc[0]
    if pd.isna(v):
        return "carga_desconhecida"
    v = float(v)
    txt = f"{int(v)}" if v.is_integer() else f"{v:g}".replace(".", ",")
    return f"{txt} kW"


def _time_diag_load_slug(load_kw: object) -> str:
    v = pd.to_numeric(pd.Series([load_kw]), errors="coerce").iloc[0]
    if pd.isna(v):
        return "carga_desconhecida"
    v = float(v)
    txt = f"{int(v)}" if v.is_integer() else f"{v:g}".replace(".", "p")
    return f"{txt}kW"


def _time_diag_has_sampling_error(dt_next: pd.Series, threshold_s: float = TIME_DELTA_ERROR_THRESHOLD_S) -> bool:
    dt_num = pd.to_numeric(dt_next, errors="coerce")
    if dt_num.notna().sum() == 0:
        return False
    return bool((dt_num > threshold_s).any())


def _time_diag_status_from_flags(flags: pd.Series) -> str:
    s = pd.Series(flags)
    valid = s.dropna()
    if valid.empty:
        return "NA"
    return "ERRO" if bool(valid.astype(bool).any()) else "OK"


def _first_last_transient_times(
    flags: pd.Series,
    time_text_ms: pd.Series,
) -> Tuple[object, object]:
    mask = pd.Series(flags).fillna(False).astype(bool)
    if mask.sum() == 0:
        return pd.NA, pd.NA

    times = pd.Series(time_text_ms)
    flagged_times = times[mask]
    if flagged_times.empty:
        return pd.NA, pd.NA

    return flagged_times.iloc[0], flagged_times.iloc[-1]


def _apply_time_delta_axis_format(ax: plt.Axes) -> None:
    ax.set_ylim(TIME_DELTA_PLOT_YMIN_S, TIME_DELTA_PLOT_YMAX_S)
    ax.set_yticks(
        np.arange(
            TIME_DELTA_PLOT_YMIN_S,
            TIME_DELTA_PLOT_YMAX_S + (TIME_DELTA_PLOT_YSTEP_S * 0.5),
            TIME_DELTA_PLOT_YSTEP_S,
        )
    )


def summarize_time_diagnostics(time_df: pd.DataFrame) -> pd.DataFrame:
    if time_df is None or time_df.empty:
        return pd.DataFrame()

    rows: List[dict] = []
    for basename, d in time_df.groupby("BaseName", dropna=False, sort=True):
        dt_next = pd.to_numeric(d["TIME_DELTA_TO_NEXT_s"], errors="coerce")
        err_ms = pd.to_numeric(d["TIME_DELTA_ERROR_ms"], errors="coerce")
        t_parsed = pd.to_datetime(d["TIME_PARSED"], errors="coerce")
        time_limit_ms = _to_float(
            d.get("MAX_DELTA_BETWEEN_SAMPLES_ms", pd.Series([DEFAULT_MAX_DELTA_BETWEEN_SAMPLES_MS])).iloc[0],
            DEFAULT_MAX_DELTA_BETWEEN_SAMPLES_MS,
        )
        time_limit_s = time_limit_ms / 1000.0
        time_flag = d.get("TIME_DELTA_ERROR_FLAG", pd.Series([pd.NA] * len(d)))
        smp_status = _time_diag_status_from_flags(time_flag)
        act_flag = d.get("ACT_CTRL_ERROR_FLAG", pd.Series([pd.NA] * len(d)))
        act_status = _time_diag_status_from_flags(act_flag)
        ect_flag = d.get("ECT_CTRL_ERROR_FLAG", pd.Series([pd.NA] * len(d)))
        ect_status = _time_diag_status_from_flags(ect_flag)
        dq_status = "ERRO" if "ERRO" in {smp_status, act_status, ect_status} else ("OK" if {smp_status, act_status, ect_status} <= {"OK"} else "NA")
        time_error_n = int(pd.Series(time_flag).fillna(False).astype(bool).sum()) if smp_status != "NA" else 0
        act_error_n = int(pd.Series(act_flag).fillna(False).astype(bool).sum()) if act_status != "NA" else 0
        ect_error_n = int(pd.Series(ect_flag).fillna(False).astype(bool).sum()) if ect_status != "NA" else 0
        act_abs = pd.to_numeric(d.get("ACT_CTRL_ERROR_ABS_C", pd.Series([pd.NA] * len(d))), errors="coerce")
        ect_abs = pd.to_numeric(d.get("ECT_CTRL_ERROR_ABS_C", pd.Series([pd.NA] * len(d))), errors="coerce")
        act_transient_status = act_status
        act_transient_t_on, act_transient_t_off = _first_last_transient_times(
            act_flag,
            d.get("TIME_TEXT_ms", pd.Series([pd.NA] * len(d))),
        )
        ect_transient_status = ect_status
        ect_transient_t_on, ect_transient_t_off = _first_last_transient_times(
            ect_flag,
            d.get("TIME_TEXT_ms", pd.Series([pd.NA] * len(d))),
        )
        max_act_error = _to_float(
            d.get("MAX_ACT_CONTROL_ERROR", pd.Series([DEFAULT_MAX_ACT_CONTROL_ERROR_C])).iloc[0],
            DEFAULT_MAX_ACT_CONTROL_ERROR_C,
        )
        max_ect_error = _to_float(
            d.get("MAX_ECT_CONTROL_ERROR", pd.Series([DEFAULT_MAX_ECT_CONTROL_ERROR_C])).iloc[0],
            DEFAULT_MAX_ECT_CONTROL_ERROR_C,
        )

        rows.append(
            {
                "Smp_ERROR": smp_status,
                "ACT_CTRL_ERRO": act_status,
                "ACT_CTRL_ERRO_TRANSIENTE": act_transient_status,
                "ACT_CTRL_ERRO_TRANSIENTE_t_on": act_transient_t_on,
                "ACT_CTRL_ERRO_TRANSIENTE_t_off": act_transient_t_off,
                "ECT_CTRL_ERRO": ect_status,
                "ECT_CTRL_ERRO_TRANSIENTE": ect_transient_status,
                "ECT_CTRL_ERRO_TRANSIENTE_t_on": ect_transient_t_on,
                "ECT_CTRL_ERRO_TRANSIENTE_t_off": ect_transient_t_off,
                "DQ_ERROR": dq_status,
                "BaseName": basename,
                "SourceFolder": d.get("SourceFolder", pd.Series([""])).iloc[0],
                "SourceFile": d.get("SourceFile", pd.Series([basename])).iloc[0],
                "Iteracao": pd.to_numeric(d.get("Iteracao", pd.Series([pd.NA])).iloc[0], errors="coerce"),
                "Sentido_Carga": d.get("Sentido_Carga", pd.Series([pd.NA])).iloc[0],
                "Load_kW": pd.to_numeric(d.get("Load_kW", pd.Series([pd.NA])).iloc[0], errors="coerce"),
                "DIES_pct": pd.to_numeric(d.get("DIES_pct", pd.Series([pd.NA])).iloc[0], errors="coerce"),
                "BIOD_pct": pd.to_numeric(d.get("BIOD_pct", pd.Series([pd.NA])).iloc[0], errors="coerce"),
                "EtOH_pct": pd.to_numeric(d.get("EtOH_pct", pd.Series([pd.NA])).iloc[0], errors="coerce"),
                "H2O_pct": pd.to_numeric(d.get("H2O_pct", pd.Series([pd.NA])).iloc[0], errors="coerce"),
                "N_samples": int(len(d)),
                "TIME_START": t_parsed.min(),
                "TIME_END": t_parsed.max(),
                "MAX_DELTA_BETWEEN_SAMPLES_ms": time_limit_ms,
                "TIME_DELTA_ERROR_N": time_error_n,
                "TIME_DELTA_ERROR_PCT": (time_error_n / len(d)) * 100.0 if len(d) > 0 else np.nan,
                "TIME_DELTA_MEDIAN_s": dt_next.median(),
                "TIME_DELTA_MEAN_s": dt_next.mean(),
                "TIME_DELTA_MIN_s": dt_next.min(),
                "TIME_DELTA_MAX_s": dt_next.max(),
                "TIME_DELTA_LIMIT_s": time_limit_s,
                "TIME_DELTA_STD_ms": dt_next.std(ddof=1) * 1000.0,
                "TIME_DELTA_MAX_ABS_ERROR_ms": err_ms.abs().max(),
                "TIME_DELTA_NONPOSITIVE_N": int((dt_next <= 0).fillna(False).sum()),
                "TIME_DELTA_MISSING_N": int(dt_next.isna().sum()),
                "MAX_ACT_CONTROL_ERROR": max_act_error,
                "ACT_CTRL_ERROR_N": act_error_n,
                "ACT_CTRL_ERROR_PCT": (act_error_n / len(d)) * 100.0 if len(d) > 0 else np.nan,
                "ACT_CTRL_ERROR_MEAN_ABS_C": act_abs.mean(),
                "ACT_CTRL_ERROR_MAX_ABS_C": act_abs.max(),
                "MAX_ECT_CONTROL_ERROR": max_ect_error,
                "ECT_CTRL_ERROR_N": ect_error_n,
                "ECT_CTRL_ERROR_PCT": (ect_error_n / len(d)) * 100.0 if len(d) > 0 else np.nan,
                "ECT_CTRL_ERROR_MEAN_ABS_C": ect_abs.mean(),
                "ECT_CTRL_ERROR_MAX_ABS_C": ect_abs.max(),
            }
        )

    out = pd.DataFrame(rows)
    if "Iteracao" in out.columns:
        out["Iteracao"] = pd.to_numeric(out["Iteracao"], errors="coerce").astype("Int64")
    return out


def plot_time_delta_all_samples(
    time_df: pd.DataFrame,
    filename: str = "time_delta_to_next_all_samples.png",
    plot_dir: Optional[Path] = None,
) -> None:
    if time_df is None or time_df.empty:
        print("[WARN] Sem dados para plot de delta T do TIME.")
        return

    d = time_df.sort_values(["BaseName", "Index"]).copy()
    x = pd.to_numeric(d["TIME_SAMPLE_GLOBAL"], errors="coerce")
    y = pd.to_numeric(d["TIME_DELTA_TO_NEXT_s"], errors="coerce")
    valid = x.notna() & y.notna()
    if valid.sum() == 0:
        print("[WARN] Sem delta T vÃ¡lido para plotar.")
        return

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(x[valid], y[valid], "-", linewidth=0.8, color="tab:blue", alpha=0.85)
    ax.scatter(x[valid], y[valid], s=8, color="tab:blue", alpha=0.45)

    median_dt = float(y[valid].median())
    ax.axhline(median_dt, color="tab:red", linestyle="--", linewidth=1.0, label=f"median={median_dt:.6f} s")
    time_limit_s = _to_float(
        d.get("TIME_DELTA_LIMIT_s", pd.Series([TIME_DELTA_ERROR_THRESHOLD_S])).iloc[0],
        TIME_DELTA_ERROR_THRESHOLD_S,
    )
    ax.axhline(
        time_limit_s,
        color="tab:orange",
        linestyle=":",
        linewidth=1.2,
        label=f"limite erro={time_limit_s:.3f} s",
    )

    ax.set_xlabel("Global sample index")
    ax.set_ylabel("Delta T to next sample (s)")
    ax.set_title("TIME delta between consecutive samples")
    _apply_time_delta_axis_format(ax)
    ax.grid(True, which="both", linestyle="--", linewidth=0.5)
    table_rows = [("", xi, yi) for xi, yi in zip(x[valid].tolist(), y[valid].tolist())]
    _add_xy_value_table(ax, table_rows)
    ax.legend()

    target_dir = PLOTS_DIR if plot_dir is None else plot_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    outpath = target_dir / filename
    fig.tight_layout()
    fig.savefig(outpath, dpi=220)
    plt.close(fig)
    print(f"[OK] Salvei {outpath}")


def plot_time_delta_by_file(time_df: pd.DataFrame, plot_dir: Optional[Path] = None) -> None:
    if time_df is None or time_df.empty:
        print("[WARN] Sem dados para plots individuais de delta T do TIME.")
        return

    base_dir = PLOTS_DIR if plot_dir is None else plot_dir
    out_dir = base_dir / "time_delta_by_file"
    out_dir.mkdir(parents=True, exist_ok=True)

    n_ok = 0
    n_skip = 0
    for basename, d in time_df.groupby("BaseName", dropna=False, sort=True):
        d = d.sort_values("Index").copy()
        x = pd.to_numeric(d["Index"], errors="coerce")
        y = pd.to_numeric(d["TIME_DELTA_TO_NEXT_s"], errors="coerce")
        valid = x.notna() & y.notna()
        if valid.sum() == 0:
            n_skip += 1
            continue

        source_folder = str(d.get("SourceFolder", pd.Series([""])).iloc[0] or "")
        source_file = str(d.get("SourceFile", pd.Series([basename])).iloc[0] or basename)
        load_kw = d.get("Load_kW", pd.Series([pd.NA])).iloc[0]
        load_title = _time_diag_load_title(load_kw)
        load_slug = _time_diag_load_slug(load_kw)
        time_limit_s = _to_float(
            d.get("TIME_DELTA_LIMIT_s", pd.Series([TIME_DELTA_ERROR_THRESHOLD_S])).iloc[0],
            TIME_DELTA_ERROR_THRESHOLD_S,
        )
        error_mask = valid & (y > time_limit_s)
        has_sampling_error = bool(error_mask.any())

        fig, ax = plt.subplots(figsize=(12, 4.5))
        ax.plot(x[valid], y[valid], "-", linewidth=0.9, color="tab:blue", alpha=0.9)
        ax.scatter(x[valid], y[valid], s=10, color="tab:blue", alpha=0.45)
        if has_sampling_error:
            ax.scatter(x[error_mask], y[error_mask], s=18, color="tab:red", alpha=0.95, label=f"delta T > {time_limit_s:.3f} s")
            ax.set_facecolor("#fff4f4")
            for spine in ax.spines.values():
                spine.set_color("tab:red")
                spine.set_linewidth(1.2)

        median_dt = float(y[valid].median())
        ax.axhline(median_dt, color="tab:red", linestyle="--", linewidth=1.0, label=f"median={median_dt:.6f} s")
        ax.axhline(
            time_limit_s,
            color="tab:orange",
            linestyle=":",
            linewidth=1.2,
            label=f"limite erro={time_limit_s:.3f} s",
        )

        title_parts = ["TIME delta entre amostras", source_file]
        if has_sampling_error:
            title_parts.insert(0, "ERRO")
        if source_folder:
            title_parts.append(source_folder)
        title_parts.append(load_title)

        ax.set_xlabel("Sample index in file")
        ax.set_ylabel("Delta T to next sample (s)")
        ax.set_title(" | ".join(title_parts))
        _apply_time_delta_axis_format(ax)
        ax.grid(True, which="both", linestyle="--", linewidth=0.5)
        table_rows = [("", xi, yi) for xi, yi in zip(x[valid].tolist(), y[valid].tolist())]
        _add_xy_value_table(ax, table_rows)
        ax.legend()

        error_prefix = "ERRO_" if has_sampling_error else ""
        filename_stem = f"{error_prefix}time_delta_to_next_{source_folder}_{load_slug}_{source_file}"
        outpath = out_dir / f"{_safe_name(filename_stem)}.png"
        fig.tight_layout()
        fig.savefig(outpath, dpi=220)
        plt.close(fig)
        print(f"[OK] Salvei {outpath}")
        n_ok += 1

    print(f"[OK] Plots TIME por arquivo: {n_ok} gerados; {n_skip} pulados.")


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
    df = _coalesce_equivalent_columns(df, context=path.name)
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
    row.update(
        {
            "Load_kW": meta.load_kw,
            "DIES_pct": meta.dies_pct,
            "BIOD_pct": meta.biod_pct,
            "EtOH_pct": meta.etoh_pct,
            "H2O_pct": meta.h2o_pct,
        }
    )
    return pd.DataFrame([row])


def kibox_aggregate(kibox_files: List[FileMeta]) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    for m in kibox_files:
        if m.load_kw is None or m.etoh_pct is None or m.h2o_pct is None:
            print(f"[WARN] Kibox sem KW/E/H no nome (nÃ£o vou agregar): {m.path.name}")
            continue
        try:
            rows.append(kibox_mean_row(m))
        except Exception as e:
            print(f"[ERROR] Kibox {m.path.name}: {e}")

    if not rows:
        return pd.DataFrame(columns=["Load_kW", "DIES_pct", "BIOD_pct", "EtOH_pct", "H2O_pct"])

    allk = pd.concat(rows, ignore_index=True)
    key_cols = ["Load_kW", "DIES_pct", "BIOD_pct", "EtOH_pct", "H2O_pct"]
    value_cols = [c for c in allk.columns if c.startswith("KIBOX_")]

    agg = allk.groupby(key_cols, dropna=False, sort=True)[value_cols].mean(numeric_only=True).reset_index()
    cnt = allk.groupby(key_cols, dropna=False, sort=True).size().reset_index(name="KIBOX_N_files")
    return agg.merge(cnt, on=key_cols, how="left")


# =========================
# Config / LHV / Instruments rev3
# =========================
def _choose_config_path() -> Path:
    p = CFG_DIR / "config_incertezas_rev3.xlsx"
    if p.exists():
        return p
    raise FileNotFoundError(f"Nao encontrei {p.name} em {CFG_DIR.resolve()}")


def _try_read_sheet(xlsx_path: Path, sheet: str) -> Optional[pd.DataFrame]:
    try:
        xf = _excel_file(xlsx_path)
        selected_sheet = None
        for s in xf.sheet_names:
            if s == sheet or str(s).strip().lower() == str(sheet).strip().lower():
                selected_sheet = s
                break
        if selected_sheet is None:
            return None
        return _read_excel(xlsx_path, sheet_name=selected_sheet)
    except Exception:
        return None


def _load_data_quality_config(xlsx_path: Path) -> Dict[str, float]:
    cfg = {
        "MAX_DELTA_BETWEEN_SAMPLES_ms": DEFAULT_MAX_DELTA_BETWEEN_SAMPLES_MS,
        "MAX_ACT_CONTROL_ERROR": DEFAULT_MAX_ACT_CONTROL_ERROR_C,
        "MAX_ECT_CONTROL_ERROR": DEFAULT_MAX_ECT_CONTROL_ERROR_C,
    }

    dqa = _try_read_sheet(xlsx_path, "data quality assessment")
    if dqa is None or dqa.empty:
        return cfg

    dqa.columns = _normalize_cols(list(dqa.columns))
    param_col = "param" if "param" in dqa.columns else (dqa.columns[0] if len(dqa.columns) >= 1 else None)
    value_col = "value" if "value" in dqa.columns else (dqa.columns[1] if len(dqa.columns) >= 2 else None)
    if param_col is None or value_col is None:
        return cfg

    for _, row in dqa.iterrows():
        param = str(row.get(param_col, "")).replace("\ufeff", "").strip()
        if not param:
            continue
        param_norm = norm_key(param)
        if param_norm == norm_key("MAX_DELTA_BETWEEN_SAMPLES_ms"):
            cfg["MAX_DELTA_BETWEEN_SAMPLES_ms"] = _to_float(
                row.get(value_col, DEFAULT_MAX_DELTA_BETWEEN_SAMPLES_MS),
                DEFAULT_MAX_DELTA_BETWEEN_SAMPLES_MS,
            )
        elif param_norm == norm_key("MAX_ACT_CONTROL_ERROR"):
            cfg["MAX_ACT_CONTROL_ERROR"] = _to_float(
                row.get(value_col, DEFAULT_MAX_ACT_CONTROL_ERROR_C),
                DEFAULT_MAX_ACT_CONTROL_ERROR_C,
            )
        elif param_norm == norm_key("MAX_ECT_CONTROL_ERROR"):
            cfg["MAX_ECT_CONTROL_ERROR"] = _to_float(
                row.get(value_col, DEFAULT_MAX_ECT_CONTROL_ERROR_C),
                DEFAULT_MAX_ECT_CONTROL_ERROR_C,
            )

    return cfg


def _load_defaults_config(xlsx_path: Path) -> Dict[str, str]:
    cfg: Dict[str, str] = {}

    defaults_df = _try_read_sheet(xlsx_path, "Defaults")
    if defaults_df is None or defaults_df.empty:
        return cfg

    defaults_df.columns = _normalize_cols(list(defaults_df.columns))
    param_col = "param" if "param" in defaults_df.columns else (defaults_df.columns[0] if len(defaults_df.columns) >= 1 else None)
    value_col = "value" if "value" in defaults_df.columns else (defaults_df.columns[1] if len(defaults_df.columns) >= 2 else None)
    if param_col is None or value_col is None:
        return cfg

    for _, row in defaults_df.iterrows():
        param = str(row.get(param_col, "")).replace("\ufeff", "").strip()
        if not param or "global parameter name" in param.lower():
            continue
        value = row.get(value_col, "")
        if pd.isna(value):
            value = ""
        cfg[norm_key(param)] = str(value).replace("\ufeff", "").strip()

    return cfg


def _resolve_runtime_dir(value: object, default_path: Path) -> Path:
    raw = str(value or "").replace("\ufeff", "").strip().strip('"').strip("'")
    if not raw:
        return default_path

    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = (BASE_DIR / p).resolve()
    return p


def _prepare_output_dir(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        return True
    except Exception:
        return False


def apply_runtime_path_overrides(defaults_cfg: Dict[str, str]) -> None:
    global RAW_DIR, PROCESS_DIR, OUT_DIR, PLOTS_DIR

    raw_input_cfg = defaults_cfg.get(norm_key("RAW_INPUT_DIR"), "")
    out_dir_cfg = defaults_cfg.get(norm_key("OUT_DIR"), "")

    input_dir = _resolve_runtime_dir(raw_input_cfg, DEFAULT_PROCESS_DIR)
    out_dir = _resolve_runtime_dir(out_dir_cfg, DEFAULT_OUT_DIR)

    if raw_input_cfg:
        print(f"[INFO] RAW_INPUT_DIR (Excel): {input_dir}")
    else:
        print(f"[INFO] RAW_INPUT_DIR vazio no Excel; usando default: {input_dir}")

    if out_dir_cfg:
        print(f"[INFO] OUT_DIR (Excel): {out_dir}")
    else:
        print(f"[INFO] OUT_DIR vazio no Excel; usando default: {out_dir}")

    if not input_dir.exists():
        raise FileNotFoundError(f"Nao encontrei o diretorio configurado em RAW_INPUT_DIR: {input_dir}")
    if not input_dir.is_dir():
        raise NotADirectoryError(f"RAW_INPUT_DIR nao aponta para um diretorio: {input_dir}")

    if not _prepare_output_dir(out_dir):
        raise FileNotFoundError(
            f"Nao consegui preparar o diretorio de saida configurado em OUT_DIR: {out_dir}"
        )

    RAW_DIR = input_dir.parent
    PROCESS_DIR = input_dir
    OUT_DIR = out_dir
    PLOTS_DIR = OUT_DIR / "plots"


def load_config_excel(xlsx_path: Optional[Path] = None) -> Tuple[dict, pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, float], Dict[str, str]]:
    p = _choose_config_path() if xlsx_path is None else xlsx_path

    m = _read_excel(p, sheet_name="Mappings")
    m.columns = _normalize_cols(list(m.columns))

    mappings: dict = {}
    for _, row in m.iterrows():
        k = norm_key(row.get("key", ""))
        col_mean_raw = str(row.get("col_mean", "")).replace("\ufeff", "").strip()
        if "logical variable identifier" in k or "exact dataframe column name" in col_mean_raw.lower():
            continue
        if not k:
            continue
        mappings[k] = {
            "mean": col_mean_raw,
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
                "y_tol_plus",
                "y_tol_minus",
                "filter_h2o_list",
                "label_variant",
                "notes",
            ]
        )

    data_quality_cfg = _load_data_quality_config(p)
    defaults_cfg = _load_defaults_config(p)

    return mappings, instruments_df, rep, plots, data_quality_cfg, defaults_cfg


def load_lhv_lookup() -> pd.DataFrame:
    p = CFG_DIR / "lhv.csv"
    if not p.exists():
        raise FileNotFoundError(f"NÃ£o encontrei {p}.")

    df = pd.read_csv(p, sep=None, engine="python", encoding="utf-8-sig")
    df.columns = _normalize_cols(list(df.columns))

    colmap: Dict[str, str] = {}
    for c in df.columns:
        cl = c.lower().strip()
        if cl in {"dies_pct", "dies", "diesel_pct", "diesel"}:
            colmap[c] = "DIES_pct"
        elif cl in {"biod_pct", "biod", "biodiesel_pct", "biodiesel"}:
            colmap[c] = "BIOD_pct"
        elif cl in {"etoh_pct", "etoh", "e_pct", "e"}:
            colmap[c] = "EtOH_pct"
        elif cl in {"h2o_pct", "h2o", "h20_pct", "h20", "h_pct", "h"}:
            colmap[c] = "H2O_pct"
        elif cl in {"lhv_kj_kg", "lhv", "pci_kj_kg", "pci"}:
            colmap[c] = "LHV_kJ_kg"
    df = df.rename(columns=colmap)

    if "LHV_kJ_kg" not in df.columns:
        raise KeyError(f"lhv.csv precisa da coluna LHV_kJ_kg. Colunas atuais: {list(df.columns)}")

    for c in COMPOSITION_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    for c in COMPOSITION_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Float64")
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

    group_cols = ["BaseName", "Load_kW", "DIES_pct", "BIOD_pct", "EtOH_pct", "H2O_pct", "WindowID"]
    ignore_cols = set(group_cols + ["Index"])
    candidate_cols = [c for c in lv_raw.columns if c not in ignore_cols]

    lv = lv_raw.copy()
    if candidate_cols:
        lv[candidate_cols] = lv[candidate_cols].apply(pd.to_numeric, errors="coerce")

    g = lv.groupby(group_cols, dropna=False, sort=True)
    n_df = g.size().reset_index(name="N_samples")
    valid_groups = n_df[n_df["N_samples"] >= MIN_SAMPLES_PER_WINDOW][group_cols].copy()
    if valid_groups.empty:
        return pd.DataFrame(columns=group_cols + ["N_samples", "Consumo_kg_h", "uB_Consumo_kg_h"])

    lv_valid = lv.merge(valid_groups, on=group_cols, how="inner")
    gv = lv_valid.groupby(group_cols, dropna=False, sort=True)

    means = gv[candidate_cols].mean(numeric_only=True).add_suffix("_mean").copy()
    first = gv[bcol].first().rename("BEtanol_start")
    last = gv[bcol].last().rename("BEtanol_end")
    n2 = gv.size().rename("N_samples")

    out = pd.concat([means, first, last, n2], axis=1).reset_index().copy()

    out["Delta_BEtanol"] = out["BEtanol_start"] - out["BEtanol_end"]
    out["DeltaT_s"] = (out["N_samples"] - 1) * DT_S
    out["Consumo_kg_h"] = (out["Delta_BEtanol"] / out["DeltaT_s"]) * 3600.0
    out.loc[out["DeltaT_s"] <= 0, "Consumo_kg_h"] = pd.NA

    bal_key = "balance_kg"
    if _has_instrument_key(instruments_df, bal_key):
        res_kg = _get_resolution_for_key(instruments_df, bal_key)
        if res_kg is None or not np.isfinite(res_kg) or res_kg <= 0:
            out["uB_Consumo_kg_h"] = pd.NA
            print("[WARN] balance_kg existe em Instruments, mas 'resolution' estÃ¡ vazio/ invÃ¡lido. uB_Consumo_kg_h ficou NA.")
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

    group_cols = ["BaseName", "Load_kW", "DIES_pct", "BIOD_pct", "EtOH_pct", "H2O_pct"]
    value_cols = [c for c in trechos.columns if c not in group_cols and c != "WindowID"]

    tre = trechos.copy()
    if value_cols:
        tre[value_cols] = tre[value_cols].apply(pd.to_numeric, errors="coerce")

    g = tre.groupby(group_cols, dropna=False, sort=True)

    mean_of_windows = g[value_cols].mean(numeric_only=True).add_suffix("_mean_of_windows").copy()
    sd_of_windows = g[value_cols].std(ddof=1, numeric_only=True).add_suffix("_sd_of_windows").copy()
    mean_of_windows.columns = [_normalize_repeated_stat_tokens_in_name(c) for c in mean_of_windows.columns]
    sd_of_windows.columns = [_normalize_repeated_stat_tokens_in_name(c) for c in sd_of_windows.columns]
    n_trechos = g.size().rename("N_trechos_validos")

    out = pd.concat([mean_of_windows, sd_of_windows, n_trechos], axis=1).reset_index().copy()

    uB_col = "uB_Consumo_kg_h"
    if uB_col in tre.columns:
        tmp = tre[group_cols + [uB_col]].copy()
        tmp[uB_col] = pd.to_numeric(tmp[uB_col], errors="coerce")

        sum_u2_df = (
            tmp.groupby(group_cols, dropna=False, sort=True)[uB_col]
            .apply(lambda s: float((s**2).sum()))
            .reset_index(name="sum_u2")
        )
        out = out.merge(sum_u2_df, on=group_cols, how="left").copy()

        N = pd.to_numeric(out["N_trechos_validos"], errors="coerce")
        out["uB_Consumo_kg_h_mean_of_windows"] = (pd.to_numeric(out["sum_u2"], errors="coerce") ** 0.5) / N
        out.drop(columns=["sum_u2"], inplace=True)
    else:
        out["uB_Consumo_kg_h_mean_of_windows"] = pd.NA

    return out.copy()


def compute_motec_trechos_stats(motec_raw: pd.DataFrame) -> pd.DataFrame:
    if motec_raw.empty:
        return pd.DataFrame()

    group_cols = ["BaseName", "Load_kW", "DIES_pct", "BIOD_pct", "EtOH_pct", "H2O_pct", "WindowID"]
    ignore_cols = set(group_cols + ["Index"])
    candidate_cols = [c for c in motec_raw.columns if c not in ignore_cols]

    mot = motec_raw.copy()
    if candidate_cols:
        mot[candidate_cols] = mot[candidate_cols].apply(pd.to_numeric, errors="coerce")

    g = mot.groupby(group_cols, dropna=False, sort=True)
    n_df = g.size().reset_index(name="Motec_N_samples")
    valid_groups = n_df[n_df["Motec_N_samples"] >= MIN_SAMPLES_PER_WINDOW][group_cols].copy()
    if valid_groups.empty:
        return pd.DataFrame(columns=group_cols + ["Motec_N_samples"])

    mot_valid = mot.merge(valid_groups, on=group_cols, how="inner")
    gv = mot_valid.groupby(group_cols, dropna=False, sort=True)

    means = gv[candidate_cols].mean(numeric_only=True).add_suffix("_mean").copy()
    n2 = gv.size().rename("Motec_N_samples")

    out = pd.concat([means, n2], axis=1).reset_index().copy()
    keep = group_cols + [c for c in out.columns if c.endswith("_mean")] + ["Motec_N_samples"]
    return out[keep].copy()


def compute_motec_ponto_stats(motec_trechos: pd.DataFrame) -> pd.DataFrame:
    if motec_trechos.empty:
        return pd.DataFrame(columns=["Load_kW", "DIES_pct", "BIOD_pct", "EtOH_pct", "H2O_pct"])

    group_cols = ["Load_kW", "DIES_pct", "BIOD_pct", "EtOH_pct", "H2O_pct"]
    value_cols = [c for c in motec_trechos.columns if c not in set(group_cols + ["BaseName", "WindowID", "Motec_N_samples"])]

    mot = motec_trechos.copy()
    if value_cols:
        mot[value_cols] = mot[value_cols].apply(pd.to_numeric, errors="coerce")

    g = mot.groupby(group_cols, dropna=False, sort=True)
    mean_of_windows = g[value_cols].mean(numeric_only=True).add_suffix("_mean_of_windows").copy()
    sd_of_windows = g[value_cols].std(ddof=1, numeric_only=True).add_suffix("_sd_of_windows").copy()
    mean_of_windows.columns = [_normalize_repeated_stat_tokens_in_name(c) for c in mean_of_windows.columns]
    sd_of_windows.columns = [_normalize_repeated_stat_tokens_in_name(c) for c in sd_of_windows.columns]
    n_trechos = g.size().rename("Motec_N_trechos_validos")
    n_files = g["BaseName"].nunique().rename("Motec_N_files")
    mean_samples = g["Motec_N_samples"].mean().rename("Motec_N_samples_mean_of_windows")

    out = pd.concat([mean_of_windows, sd_of_windows, n_trechos, n_files, mean_samples], axis=1).reset_index().copy()
    return out


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
            print(f"[WARN] Uncertainty: key='{key_norm}' col_mean '{col_mean_req}' nÃ£o encontrada no output. Pulando. ({e})")
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


def _normalized_composition_keys(df: pd.DataFrame) -> pd.DataFrame:
    idx = df.index
    out = pd.DataFrame(index=idx)

    dies = pd.to_numeric(df.get("DIES_pct", pd.Series(pd.NA, index=idx)), errors="coerce")
    biod = pd.to_numeric(df.get("BIOD_pct", pd.Series(pd.NA, index=idx)), errors="coerce")
    etoh = pd.to_numeric(df.get("EtOH_pct", pd.Series(pd.NA, index=idx)), errors="coerce")
    h2o = pd.to_numeric(df.get("H2O_pct", pd.Series(pd.NA, index=idx)), errors="coerce")

    has_diesel = dies.notna() | biod.notna()
    has_ethanol = etoh.notna() | h2o.notna()

    out["DIES_pct"] = dies.where(has_diesel, np.where(has_ethanol, 0.0, np.nan))
    out["BIOD_pct"] = biod.where(has_diesel, np.where(has_ethanol, 0.0, np.nan))
    out["EtOH_pct"] = etoh.where(has_ethanol, np.where(has_diesel, 0.0, np.nan))
    out["H2O_pct"] = h2o.where(has_ethanol, np.where(has_diesel, 0.0, np.nan))
    return out


def _left_merge_on_fuel_keys(left: pd.DataFrame, right: pd.DataFrame, extra_on: Optional[List[str]] = None) -> pd.DataFrame:
    extra = extra_on or []

    l = left.copy()
    r = right.copy()

    l_norm = _normalized_composition_keys(l)
    r_norm = _normalized_composition_keys(r)

    merge_cols: List[str] = []
    for c in extra + COMPOSITION_COLS:
        tmp = f"__merge_{c}"
        if c in extra:
            l[tmp] = pd.to_numeric(l[c], errors="coerce")
            r[tmp] = pd.to_numeric(r[c], errors="coerce")
        else:
            l[tmp] = pd.to_numeric(l_norm[c], errors="coerce")
            r[tmp] = pd.to_numeric(r_norm[c], errors="coerce")
        merge_cols.append(tmp)

    right_payload = r.drop(columns=[c for c in extra + COMPOSITION_COLS if c in r.columns]).copy()
    for tmp in merge_cols:
        right_payload[tmp] = r[tmp]

    out = l.merge(right_payload, on=merge_cols, how="left")
    out.drop(columns=merge_cols, inplace=True)
    return out


def _guess_plot_uncertainty_col(out_df: pd.DataFrame, y_col: str, mappings: dict) -> Optional[str]:
    candidates: List[str] = []

    direct = f"U_{y_col}"
    if direct not in candidates:
        candidates.append(direct)

    for key_norm, spec in mappings.items():
        col_mean_req = str(spec.get("mean", "")).strip()
        if not col_mean_req:
            continue
        try:
            mapped_mean = resolve_col(out_df, col_mean_req)
        except Exception:
            continue
        if mapped_mean == y_col:
            cand = f"U_{_prefix_from_key_norm(key_norm)}"
            if cand not in candidates:
                candidates.append(cand)

    for cand in candidates:
        if cand not in out_df.columns:
            continue
        vals = pd.to_numeric(out_df[cand], errors="coerce")
        if vals.notna().any():
            return cand
    return None


# =========================
# Final table
# =========================
def build_final_table(
    ponto: pd.DataFrame,
    lhv: pd.DataFrame,
    kibox_agg: pd.DataFrame,
    motec_ponto: pd.DataFrame,
    mappings: dict,
    instruments_df: pd.DataFrame,
    reporting_df: pd.DataFrame,
) -> pd.DataFrame:
    df = _left_merge_on_fuel_keys(ponto, lhv)
    if kibox_agg is not None and not kibox_agg.empty:
        df = _left_merge_on_fuel_keys(df, kibox_agg, extra_on=["Load_kW"])
    if motec_ponto is not None and not motec_ponto.empty:
        df = _left_merge_on_fuel_keys(df, motec_ponto, extra_on=["Load_kW"])

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
            print(f"[WARN] NÃ£o calculei MFB_10_90: ai90_col={ai90_col}, ai10_col={ai10_col}")

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

    # Generic alias for the measured UPD power used by runtime-specific plots.
    df["UPD_Power_kW"] = PkW
    df["UPD_Power_Bin_kW"] = PkW.round(1).where(PkW.notna(), pd.NA)

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

    # Specific fuel consumption (g/kWh): BSFC = 1000 * fuel_kg_h / power_kW.
    bsfc = (Fkgh * 1000.0) / PkW
    invalid_bsfc = (PkW <= 0) | (Fkgh <= 0)
    df["BSFC_g_kWh"] = bsfc.where(~invalid_bsfc, pd.NA)

    uA_P = pd.to_numeric(df.get("uA_P_kw", pd.NA), errors="coerce")
    uB_P = pd.to_numeric(df.get("uB_P_kw", pd.NA), errors="coerce")
    uA_F = pd.to_numeric(df.get("uA_Consumo_kg_h", pd.NA), errors="coerce")
    uB_F = pd.to_numeric(df.get("uB_Consumo_kg_h", pd.NA), errors="coerce")

    rel_uA_bsfc = ((uA_F / Fkgh) ** 2 + (uA_P / PkW) ** 2) ** 0.5
    rel_uB_bsfc = ((uB_F / Fkgh) ** 2 + (uB_P / PkW) ** 2) ** 0.5

    df["uA_BSFC_g_kWh"] = pd.to_numeric(df["BSFC_g_kWh"], errors="coerce") * rel_uA_bsfc
    df["uB_BSFC_g_kWh"] = pd.to_numeric(df["BSFC_g_kWh"], errors="coerce") * rel_uB_bsfc
    ua_bsfc = pd.to_numeric(df["uA_BSFC_g_kWh"], errors="coerce")
    ub_bsfc = pd.to_numeric(df["uB_BSFC_g_kWh"], errors="coerce")
    df["uc_BSFC_g_kWh"] = (ua_bsfc**2 + ub_bsfc**2) ** 0.5
    df["U_BSFC_g_kWh"] = K_COVERAGE * df["uc_BSFC_g_kWh"]

    df.loc[invalid_bsfc, ["uA_BSFC_g_kWh", "uB_BSFC_g_kWh", "uc_BSFC_g_kWh", "U_BSFC_g_kWh"]] = pd.NA

    lambda_col = None
    if "lambda" in mappings and mappings["lambda"].get("mean"):
        try:
            lambda_col = resolve_col(df, mappings["lambda"]["mean"])
        except Exception:
            lambda_col = None
    df = add_airflow_channels_inplace(df, lambda_col=lambda_col)

    t_cil_cols = [
        "T_S_CIL_1_mean_of_windows",
        "T_S_CIL_2_mean_of_windows",
        "T_S_CIL_3_mean_of_windows",
        "T_S_CIL_4_mean_of_windows",
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

    # ECT control error sign convention:
    # positive error => coolant outlet temperature hotter than commanded setpoint.
    t_s_agua_col = None
    for cand in [
        "T_S_AGUA_mean_of_windows",
        "T_S_AGUA",
        "T_S_ÃGUA",
    ]:
        if cand in df.columns:
            t_s_agua_col = cand
            break
    if t_s_agua_col is None:
        t_s_agua_col = _find_first_col_by_substrings(df, ["t_s", "agua", "mean_of_windows"])
    if t_s_agua_col is None:
        t_s_agua_col = _find_first_col_by_substrings(df, ["t_s", "agua"])
    if t_s_agua_col is None:
        t_s_agua_col = _find_first_col_by_substrings(df, ["t_s", "Ã¡gua"])

    dem_th2o_col = None
    for cand in [
        "DEM_TH2O_mean_of_windows",
        "DEM TH2O_mean_of_windows",
        "DEM_TH2O",
        "DEM TH2O",
    ]:
        if cand in df.columns:
            dem_th2o_col = cand
            break
    if dem_th2o_col is None:
        dem_th2o_col = _find_first_col_by_substrings(df, ["dem", "th2o", "mean_of_windows"])
    if dem_th2o_col is None:
        dem_th2o_col = _find_first_col_by_substrings(df, ["dem", "th2o"])

    if t_s_agua_col and dem_th2o_col:
        ect_actual = pd.to_numeric(df[t_s_agua_col], errors="coerce")
        ect_target = pd.to_numeric(df[dem_th2o_col], errors="coerce")
        df["ECT_CTRL_ACTUAL_C"] = ect_actual
        df["ECT_CTRL_TARGET_C"] = ect_target
        df["ECT_CTRL_ERROR_C"] = ect_actual - ect_target
        df["ECT_CTRL_ERROR_ABS_C"] = pd.to_numeric(df["ECT_CTRL_ERROR_C"], errors="coerce").abs()
    else:
        df["ECT_CTRL_ACTUAL_C"] = pd.NA
        df["ECT_CTRL_TARGET_C"] = pd.NA
        df["ECT_CTRL_ERROR_C"] = pd.NA
        df["ECT_CTRL_ERROR_ABS_C"] = pd.NA

    # Ignition delay (absolute delta in crank angle):
    # - MoTeC ignition timing is positive for BTDC.
    # - KIBOX AI05 is positive for ATDC.
    # Convert both to a common ATDC-oriented axis by flipping MoTeC sign.
    # Therefore: delay_abs = abs(AI05_ATDC - (-Ignition_BTDC)) = abs(AI05 + Ignition).
    motec_ign_col = "Motec_Ignition Timing_mean_of_windows"
    kibox_ai05_col = "KIBOX_AI05_1"
    motec_ign = pd.to_numeric(df.get(motec_ign_col, pd.NA), errors="coerce")
    kibox_ai05 = pd.to_numeric(df.get(kibox_ai05_col, pd.NA), errors="coerce")
    delay_abs = (kibox_ai05 + motec_ign).abs()
    df["Ignition_Delay_abs_degCA"] = delay_abs.where(motec_ign.notna() & kibox_ai05.notna(), pd.NA)

    df = add_run_context_columns(df)
    df = _apply_reporting_rounding(df, mappings=mappings, reporting_df=reporting_df)

    # Keep run context columns at the beginning of the final spreadsheet.
    first_cols = [c for c in ["Iteracao", "Sentido_Carga"] if c in df.columns]
    if first_cols:
        rest_cols = [c for c in df.columns if c not in first_cols]
        df = df[first_cols + rest_cols].copy()

    return df


# =========================
# Plot primitives
# =========================
def _fuel_plot_groups(df: pd.DataFrame, fuels_override: Optional[List[int]] = None) -> List[Tuple[Optional[str], pd.DataFrame]]:
    if "H2O_pct" not in df.columns:
        return [(None, df.copy())]

    h2o = pd.to_numeric(df["H2O_pct"], errors="coerce")
    if h2o.notna().sum() == 0:
        return [(None, df.copy())]

    fuels = fuels_override if fuels_override is not None else sorted(float(v) for v in h2o.dropna().unique())
    groups: List[Tuple[Optional[str], pd.DataFrame]] = []

    for h in fuels:
        hv = float(h)
        d = df[h2o.eq(hv)].copy()
        if d.empty:
            continue

        label = f"H2O={int(hv)}%" if hv.is_integer() else f"H2O={hv:g}%"
        groups.append((label, d))

    return groups


def _series_fuel_plot_groups(
    df: pd.DataFrame,
    fuels_override: Optional[List[int]] = None,
    series_col: Optional[str] = None,
) -> List[Tuple[Optional[str], pd.DataFrame]]:
    if not series_col or series_col not in df.columns:
        return _fuel_plot_groups(df, fuels_override=fuels_override)

    sv = df[series_col].map(_to_str_or_empty)
    sv = sv.where(sv.ne(""), "origem_desconhecida")

    groups: List[Tuple[Optional[str], pd.DataFrame]] = []
    for serie in sorted(sv.dropna().unique().tolist()):
        d_series = df[sv.eq(serie)].copy()
        if d_series.empty:
            continue
        for fuel_label, d in _fuel_plot_groups(d_series, fuels_override=fuels_override):
            if d.empty:
                continue
            label = str(serie)
            if fuel_label:
                label = f"{serie} | {fuel_label}"
            groups.append((label, d))

    if groups:
        return groups
    return _fuel_plot_groups(df, fuels_override=fuels_override)


def _normalize_tol_value(v: object) -> float:
    x = _to_float(v, 0.0)
    if not np.isfinite(x):
        return 0.0
    return abs(float(x))


def _add_y_tolerance_guides(ax: plt.Axes, y_tol_plus: object, y_tol_minus: object) -> int:
    tp = _normalize_tol_value(y_tol_plus)
    tm = _normalize_tol_value(y_tol_minus)
    n = 0
    if tp > 0:
        ax.axhline(tp, color="red", linestyle="--", linewidth=1.2, label=f"limite +{tp:g}")
        n += 1
    if tm > 0:
        ax.axhline(-tm, color="red", linestyle="--", linewidth=1.2, label=f"limite -{tm:g}")
        n += 1
    return n


def _fmt_table_number(v: object) -> str:
    x = _to_float(v, default=np.nan)
    if not np.isfinite(x):
        return ""
    if abs(x) >= 1000 or (abs(x) > 0 and abs(x) < 0.01):
        return f"{x:.3e}"
    return f"{x:.3f}"


def _add_xy_value_table(
    ax: plt.Axes,
    rows: List[Tuple[str, object, object]],
    max_rows: int = 12,
) -> None:
    # Tabelas embutidas nos plots foram desativadas por decisao de usabilidade.
    return


def _apply_y_tick_step(ax: plt.Axes, y_tick_step: Optional[float]) -> None:
    step = _to_float(y_tick_step, default=np.nan)
    if not np.isfinite(step) or step <= 0:
        return

    ymin, ymax = ax.get_ylim()
    if not (np.isfinite(ymin) and np.isfinite(ymax)):
        return

    eps = abs(step) * 1e-9
    first = np.ceil((ymin - eps) / step) * step
    last = np.floor((ymax + eps) / step) * step
    if not (np.isfinite(first) and np.isfinite(last)) or last < first:
        return

    ticks = np.arange(first, last + (step * 0.5), step).tolist()
    if not ticks:
        return

    ax.set_yticks(ticks)
    ax.set_ylim(ymin, ymax)


def plot_all_fuels(
    df: pd.DataFrame,
    y_col: str,
    yerr_col: Optional[str],
    title: str,
    filename: str,
    y_label: str,
    fixed_y: Optional[Tuple[float, float, float]] = None,
    y_tick_step: Optional[float] = None,
    fixed_x: Optional[Tuple[float, float, float]] = None,
    x_col: str = "Load_kW",
    x_label: str = "Power (kW)",
    fuels_override: Optional[List[int]] = None,
    series_col: Optional[str] = None,
    plot_dir: Optional[Path] = None,
    y_tol_plus: object = 0.0,
    y_tol_minus: object = 0.0,
) -> None:
    target_dir = PLOTS_DIR if plot_dir is None else plot_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    plt.figure()
    any_curve = False
    legend_entries = 0
    table_rows: List[Tuple[str, object, object]] = []
    table_rows: List[Tuple[str, object, object]] = []

    for label, d in _series_fuel_plot_groups(df, fuels_override=fuels_override, series_col=series_col):
        d[x_col] = pd.to_numeric(d[x_col], errors="coerce")
        d[y_col] = pd.to_numeric(d[y_col], errors="coerce")
        if yerr_col:
            d[yerr_col] = pd.to_numeric(d[yerr_col], errors="coerce")
            d = d.dropna(subset=[x_col, y_col, yerr_col]).sort_values(x_col)
        else:
            d = d.dropna(subset=[x_col, y_col]).sort_values(x_col)

        if d.empty:
            continue

        for xi, yi in zip(d[x_col].tolist(), d[y_col].tolist()):
            table_rows.append((label or "", xi, yi))

        any_curve = True
        if yerr_col:
            if label:
                plt.errorbar(d[x_col], d[y_col], yerr=d[yerr_col], fmt="o-", capsize=3, label=label)
                legend_entries += 1
            else:
                plt.errorbar(d[x_col], d[y_col], yerr=d[yerr_col], fmt="o-", capsize=3)
        else:
            if label:
                plt.plot(d[x_col], d[y_col], "o-", label=label)
                legend_entries += 1
            else:
                plt.plot(d[x_col], d[y_col], "o-")

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

    ax = plt.gca()
    guide_entries = _add_y_tolerance_guides(ax, y_tol_plus=y_tol_plus, y_tol_minus=y_tol_minus)
    if fixed_y is None:
        _apply_y_tick_step(ax, y_tick_step)

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    _add_xy_value_table(ax, table_rows)
    if legend_entries > 0 or guide_entries > 0:
        plt.legend()
    outpath = target_dir / filename
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
    y_tick_step: Optional[float] = None,
    fixed_x: Optional[Tuple[float, float, float]] = None,
    fuels_override: Optional[List[int]] = None,
    series_col: Optional[str] = None,
    plot_dir: Optional[Path] = None,
    y_tol_plus: object = 0.0,
    y_tol_minus: object = 0.0,
) -> None:
    target_dir = PLOTS_DIR if plot_dir is None else plot_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    plt.figure()
    any_curve = False
    legend_entries = 0
    table_rows: List[Tuple[str, object, object]] = []

    for label, d in _series_fuel_plot_groups(df, fuels_override=fuels_override, series_col=series_col):
        d[x_col] = pd.to_numeric(d[x_col], errors="coerce")
        d[y_col] = pd.to_numeric(d[y_col], errors="coerce")
        if yerr_col:
            d[yerr_col] = pd.to_numeric(d[yerr_col], errors="coerce")
            d = d.dropna(subset=[x_col, y_col, yerr_col]).sort_values(x_col)
        else:
            d = d.dropna(subset=[x_col, y_col]).sort_values(x_col)

        if d.empty:
            continue

        for xi, yi in zip(d[x_col].tolist(), d[y_col].tolist()):
            table_rows.append((label or "", xi, yi))

        any_curve = True
        if yerr_col:
            if label:
                plt.errorbar(d[x_col], d[y_col], yerr=d[yerr_col], fmt="o-", capsize=3, label=label)
                legend_entries += 1
            else:
                plt.errorbar(d[x_col], d[y_col], yerr=d[yerr_col], fmt="o-", capsize=3)
        else:
            if label:
                plt.plot(d[x_col], d[y_col], "o-", label=label)
                legend_entries += 1
            else:
                plt.plot(d[x_col], d[y_col], "o-")

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

    ax = plt.gca()
    guide_entries = _add_y_tolerance_guides(ax, y_tol_plus=y_tol_plus, y_tol_minus=y_tol_minus)
    if fixed_y is None:
        _apply_y_tick_step(ax, y_tick_step)

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    _add_xy_value_table(ax, table_rows)
    if legend_entries > 0 or guide_entries > 0:
        plt.legend()
    outpath = target_dir / filename
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
    y_tick_step: Optional[float] = None,
    fixed_x: Optional[Tuple[float, float, float]] = None,
    x_col: str = "Load_kW",
    x_label: str = "Power (kW)",
    fuels_override: Optional[List[int]] = None,
    series_col: Optional[str] = None,
    plot_dir: Optional[Path] = None,
    y_tol_plus: object = 0.0,
    y_tol_minus: object = 0.0,
) -> None:
    target_dir = PLOTS_DIR if plot_dir is None else plot_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots()
    any_curve = False
    legend_entries = 0
    table_rows: List[Tuple[str, object, object]] = []

    for label, d in _series_fuel_plot_groups(df, fuels_override=fuels_override, series_col=series_col):
        d[x_col] = pd.to_numeric(d[x_col], errors="coerce")
        d[y_col] = pd.to_numeric(d[y_col], errors="coerce")
        d = d.dropna(subset=[x_col, y_col]).sort_values(x_col)

        if d.empty:
            continue

        for xi, yi in zip(d[x_col].tolist(), d[y_col].tolist()):
            table_rows.append((label or "", xi, yi))

        any_curve = True
        if label:
            ax.plot(d[x_col], d[y_col], "o-", label=label)
            legend_entries += 1
        else:
            ax.plot(d[x_col], d[y_col], "o-")

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

    guide_entries = _add_y_tolerance_guides(ax, y_tol_plus=y_tol_plus, y_tol_minus=y_tol_minus)
    if fixed_y is None:
        _apply_y_tick_step(ax, y_tick_step)

    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_title(title)
    ax.grid(True, which="both", linestyle="--", linewidth=0.5)
    _add_xy_value_table(ax, table_rows)
    if legend_entries > 0 or guide_entries > 0:
        ax.legend()

    outpath = target_dir / filename
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


def _yerr_disabled_token(s: str) -> bool:
    t = str(s or "").strip().lower()
    return t in {"none", "off", "disable", "disabled", "0", "na", "n/a"}


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


def _resolve_plot_x_request(x_col_req: str) -> Tuple[str, bool]:
    req = _to_str_or_empty(x_col_req)
    req_norm = norm_key(req) if req else ""
    load_norm = norm_key("Load_kW")
    if is_mestrado_runtime() and (not req or req_norm == load_norm):
        return "UPD_Power_Bin_kW", True
    return ("Load_kW" if not req else req), False


def _runtime_plot_x_label(
    x_label: str,
    x_col_base: str,
    x_col_resolved: str,
    mestrado_override: bool,
) -> str:
    label = _to_str_or_empty(x_label)
    if mestrado_override:
        label_norm = norm_key(label) if label else ""
        x_base_norm = norm_key(x_col_base)
        if not label or label_norm in {
            x_base_norm,
            norm_key("Load_kW"),
            norm_key("Carga (kW)"),
            norm_key("Power (kW)"),
            norm_key("Power"),
            norm_key("Potencia (kW)"),
        }:
            return "Potencia UPD medida (kW, bin 0.1)"
    return label if label else x_col_resolved


def make_plots_from_config(
    out_df: pd.DataFrame,
    plots_df: pd.DataFrame,
    mappings: dict,
    plot_dir: Optional[Path] = None,
    series_col: Optional[str] = None,
) -> None:
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
        print("[WARN] Plots config vazio; nÃ£o gerei plots via planilha.")
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
            print("[ERROR] Plots row invÃ¡lida: plot_type vazio. Pulei.")
            n_skip += 1
            continue

        x_col_req = _to_str_or_empty(r.get("x_col", ""))
        y_col_req = _to_str_or_empty(r.get("y_col", ""))
        yerr_req = _to_str_or_empty(r.get("yerr_col", ""))

        x_label = _to_str_or_empty(r.get("x_label", ""))
        y_label = _to_str_or_empty(r.get("y_label", ""))

        fixed_x = _parse_axis_spec(r.get("x_min", pd.NA), r.get("x_max", pd.NA), r.get("x_step", pd.NA))
        fixed_y = _parse_axis_spec(r.get("y_min", pd.NA), r.get("y_max", pd.NA), r.get("y_step", pd.NA))
        y_tick_step = _to_float(r.get("y_step", pd.NA), np.nan)
        if not np.isfinite(y_tick_step) or y_tick_step <= 0:
            y_tick_step = None
        if fixed_y is not None:
            y_tick_step = None
        y_tol_plus = _to_float(r.get("y_tol_plus", r.get("tol_plus", 0.0)), 0.0)
        y_tol_minus = _to_float(r.get("y_tol_minus", r.get("tol_minus", 0.0)), 0.0)

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
                print("[WARN] kibox_all: nÃ£o hÃ¡ colunas KIBOX_* no output. Pulei expansÃ£o.")
                n_skip += 1
                continue

            # x column default for kibox_all
            x_col_base, mestrado_x_override = _resolve_plot_x_request(x_col_req)
            try:
                x_col = resolve_col(out_df, x_col_base)
            except Exception as e:
                print(f"[ERROR] kibox_all: x_col '{x_col_base}' nÃ£o encontrado. Pulei expansÃ£o. ({e})")
                n_skip += 1
                continue

            xlab = _runtime_plot_x_label(x_label, x_col_base, x_col, mestrado_x_override)
            seen_filenames: set[str] = set()

            for yc in sorted(kibox_cols):
                fn = _derive_filename_for_expansion(filename, yc)
                fn_key = norm_key(fn)
                if fn_key in seen_filenames:
                    print(f"[INFO] kibox_all: filename duplicado apos normalizacao ('{fn}'). Pulei a expansao de '{yc}'.")
                    continue
                seen_filenames.add(fn_key)
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
                    y_tick_step=y_tick_step,
                    fixed_x=fixed_x,
                    x_col=x_col,
                    x_label=xlab,
                    fuels_override=fuels_override,
                    series_col=series_col,
                    plot_dir=plot_dir,
                    y_tol_plus=y_tol_plus,
                    y_tol_minus=y_tol_minus,
                )
                n_ok += 1
            continue

        # ---------
        # Normal plots (one output per row)
        # ---------
        if pt in {"all_fuels_yx", "all_fuels", "all_fuels_y_vs_x"}:
            x_col_base, mestrado_x_override = _resolve_plot_x_request(x_col_req)
            try:
                x_col = resolve_col(out_df, x_col_base)
            except Exception as e:
                print(f"[ERROR] Plot '{filename or title}': x_col '{x_col_base}' nÃ£o encontrado. Pulei. ({e})")
                n_skip += 1
                continue

            if not y_col_req:
                print(f"[ERROR] Plot '{filename or title}': y_col vazio. Pulei.")
                n_skip += 1
                continue
            try:
                y_col = resolve_col(out_df, y_col_req)
            except Exception as e:
                print(f"[ERROR] Plot '{filename or title}': y_col '{y_col_req}' nÃ£o encontrado. Pulei. ({e})")
                n_skip += 1
                continue

            yerr_col: Optional[str] = None
            if yerr_req:
                if _yerr_disabled_token(yerr_req):
                    yerr_col = None
                else:
                    try:
                        yerr_col = resolve_col(out_df, yerr_req)
                    except Exception:
                        yerr_col = None
                        print(f"[INFO] Plot \'{filename or title}\': yerr_col \'{yerr_req}\' n?o encontrado. Vou plotar sem erro.")
            else:
                yerr_col = _guess_plot_uncertainty_col(out_df, y_col, mappings)
                if yerr_col:
                    print(f"[INFO] Plot '{filename or title}': usando '{yerr_col}' como incerteza final.")

            x_label = _runtime_plot_x_label(x_label, x_col_base, x_col, mestrado_x_override)
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
                y_tick_step=y_tick_step,
                fixed_x=fixed_x,
                x_col=x_col,
                x_label=x_label,
                fuels_override=fuels_override,
                series_col=series_col,
                plot_dir=plot_dir,
                y_tol_plus=y_tol_plus,
                y_tol_minus=y_tol_minus,
            )
            n_ok += 1
            continue

        if pt in {"all_fuels_xy", "xy"}:
            if not y_col_req:
                print(f"[ERROR] Plot '{filename or title}': y_col vazio (plot_type=all_fuels_xy). Pulei.")
                n_skip += 1
                continue

            x_col_base, mestrado_x_override = _resolve_plot_x_request(x_col_req)
            try:
                x_col = resolve_col(out_df, x_col_base)
            except Exception as e:
                print(f"[ERROR] Plot '{filename or title}': x_col '{x_col_base}' nÃ£o encontrado. Pulei. ({e})")
                n_skip += 1
                continue

            try:
                y_col = resolve_col(out_df, y_col_req)
            except Exception as e:
                print(f"[ERROR] Plot '{filename or title}': y_col '{y_col_req}' nÃ£o encontrado. Pulei. ({e})")
                n_skip += 1
                continue

            yerr_col = None
            if yerr_req:
                if _yerr_disabled_token(yerr_req):
                    yerr_col = None
                else:
                    try:
                        yerr_col = resolve_col(out_df, yerr_req)
                    except Exception:
                        yerr_col = None
                        print(f"[INFO] Plot \'{filename or title}\': yerr_col \'{yerr_req}\' n?o encontrado. Vou plotar sem erro.")
            else:
                yerr_col = _guess_plot_uncertainty_col(out_df, y_col, mappings)
                if yerr_col:
                    print(f"[INFO] Plot '{filename or title}': usando '{yerr_col}' como incerteza final.")

            x_label = _runtime_plot_x_label(x_label, x_col_base, x_col, mestrado_x_override)
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
                y_tick_step=y_tick_step,
                fixed_x=fixed_x,
                fuels_override=fuels_override,
                series_col=series_col,
                plot_dir=plot_dir,
                y_tol_plus=y_tol_plus,
                y_tol_minus=y_tol_minus,
            )
            n_ok += 1
            continue

        if pt in {"all_fuels_labels", "labels"}:
            x_col_base, mestrado_x_override = _resolve_plot_x_request(x_col_req)
            try:
                x_col = resolve_col(out_df, x_col_base)
            except Exception as e:
                print(f"[ERROR] Plot '{filename or title}': x_col '{x_col_base}' nÃ£o encontrado. Pulei. ({e})")
                n_skip += 1
                continue

            if not y_col_req:
                print(f"[ERROR] Plot '{filename or title}': y_col vazio (plot_type=all_fuels_labels). Pulei.")
                n_skip += 1
                continue
            try:
                y_col = resolve_col(out_df, y_col_req)
            except Exception as e:
                print(f"[ERROR] Plot '{filename or title}': y_col '{y_col_req}' nÃ£o encontrado. Pulei. ({e})")
                n_skip += 1
                continue

            x_label = _runtime_plot_x_label(x_label, x_col_base, x_col, mestrado_x_override)
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
                y_tick_step=y_tick_step,
                fixed_x=fixed_x,
                x_col=x_col,
                x_label=x_label,
                fuels_override=fuels_override,
                series_col=series_col,
                plot_dir=plot_dir,
                y_tol_plus=y_tol_plus,
                y_tol_minus=y_tol_minus,
            )
            n_ok += 1
            continue

        print(f"[ERROR] Plot '{filename or title}': plot_type '{plot_type}' nÃ£o suportado. Pulei.")
        n_skip += 1

    print(f"[OK] Plots-config: {n_ok} gerados; {n_skip} pulados.")


# =========================
# Main
# =========================
def main() -> None:
    print(f"[INFO] Base do script: {BASE_DIR}")
    config_path = _choose_config_path()
    mappings, instruments_df, reporting_df, plots_df, data_quality_cfg, defaults_cfg = load_config_excel(config_path)
    apply_runtime_path_overrides(defaults_cfg)
    print(f"[INFO] Config: {config_path}")
    print(f"[INFO] Entrada LabVIEW/Kibox: {PROCESS_DIR}")
    print(f"[INFO] Saida: {OUT_DIR}")
    clear_output_dir(OUT_DIR)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    raw_files = [p for p in PROCESS_DIR.rglob("*") if p.is_file() and not p.name.startswith("~$")]
    metas = [parse_meta(p) for p in raw_files]

    lv_files = [m for m in metas if m.source_type == "LABVIEW" and m.path.suffix.lower() == ".xlsx"]
    kibox_files = [m for m in metas if m.source_type == "KIBOX" and m.path.suffix.lower() == ".csv"]
    motec_files = [m for m in metas if m.source_type == "MOTEC" and m.path.suffix.lower() == ".csv"]

    if not lv_files:
        raise SystemExit(f"NÃ£o achei .xlsx do LabVIEW em {PROCESS_DIR}.")

    missing_comp = [m.basename for m in lv_files if m.composition_parse == "missing_filename"]
    if missing_comp:
        preview = ", ".join(missing_comp[:5])
        suffix = " ..." if len(missing_comp) > 5 else ""
        print(
            f"[INFO] {len(missing_comp)} arquivo(s) sem composiÃ§Ã£o no nome; "
            f"DIES_pct/BIOD_pct/EtOH_pct/H2O_pct ficarÃ£o em branco no output. Exemplos: {preview}{suffix}"
        )

    ambiguous_load = [m.basename for m in lv_files if m.load_parse == "ambiguous_filename"]
    if ambiguous_load:
        preview = ", ".join(ambiguous_load[:5])
        suffix = " ..." if len(ambiguous_load) > 5 else ""
        print(
            f"[INFO] {len(ambiguous_load)} arquivo(s) com mÃºltiplas cargas no nome; "
            f"vou inferir Load_kW pela coluna de carga. Exemplos: {preview}{suffix}"
        )

    missing_load = [m.basename for m in lv_files if m.load_parse == "missing_filename"]
    if missing_load:
        preview = ", ".join(missing_load[:5])
        suffix = " ..." if len(missing_load) > 5 else ""
        print(
            f"[INFO] {len(missing_load)} arquivo(s) sem carga explÃ­cita no nome; "
            f"vou tentar inferir Load_kW pela coluna de carga. Exemplos: {preview}{suffix}"
        )

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
    lv_time_diag = build_time_diagnostics(lv_raw, quality_cfg=data_quality_cfg)
    if not lv_time_diag.empty:
        time_diag_out = lv_time_diag.copy()
        time_diag_out["Iteracao"] = pd.to_numeric(time_diag_out.get("Iteracao", pd.NA), errors="coerce").astype("Int64")
        time_diag_out["_SENTIDO_rank"] = time_diag_out.get("Sentido_Carga", pd.Series([pd.NA] * len(time_diag_out))).map(_sentido_carga_rank).fillna(9)

        diag_first_cols = [c for c in ["Iteracao", "Sentido_Carga", "Load_kW"] if c in time_diag_out.columns]
        if diag_first_cols:
            diag_rest = [c for c in time_diag_out.columns if c not in diag_first_cols]
            time_diag_out = time_diag_out[diag_first_cols + diag_rest].copy()

        time_diag_xlsx = safe_to_excel(
            time_diag_out.sort_values(
                ["Iteracao", "_SENTIDO_rank", "Load_kW", "BaseName", "Index"],
                ascending=[True, True, True, True, True],
                na_position="last",
            ).drop(columns=["_SENTIDO_rank"]).copy(),
            OUT_DIR / "lv_time_diagnostics.xlsx",
        )
        print(f"[OK] DiagnÃ³stico de qualidade por amostra gerado: {time_diag_xlsx}")

        lv_time_summary = summarize_time_diagnostics(lv_time_diag)
        if not lv_time_summary.empty:
            summary_out = add_run_context_columns(lv_time_summary.copy())
            summary_out["Iteracao"] = pd.to_numeric(summary_out.get("Iteracao", pd.NA), errors="coerce").astype("Int64")
            status_rank = {"ERRO": 0, "OK": 1, "NA": 2}
            summary_out["_DQ_rank"] = summary_out["DQ_ERROR"].map(status_rank).fillna(9)
            summary_out["_SMP_rank"] = summary_out["Smp_ERROR"].map(status_rank).fillna(9)
            summary_out["_ACT_rank"] = summary_out["ACT_CTRL_ERRO"].map(status_rank).fillna(9)
            summary_out["_ACT_TRANS_rank"] = summary_out["ACT_CTRL_ERRO_TRANSIENTE"].map(status_rank).fillna(9)
            summary_out["_ECT_rank"] = summary_out.get("ECT_CTRL_ERRO", pd.Series([pd.NA] * len(summary_out))).map(status_rank).fillna(9)
            summary_out["_ECT_TRANS_rank"] = summary_out.get("ECT_CTRL_ERRO_TRANSIENTE", pd.Series([pd.NA] * len(summary_out))).map(status_rank).fillna(9)
            summary_out["_SENTIDO_rank"] = summary_out.get("Sentido_Carga", pd.Series([pd.NA] * len(summary_out))).map(_sentido_carga_rank).fillna(9)

            sum_first_cols = [c for c in ["Iteracao", "Sentido_Carga", "Load_kW"] if c in summary_out.columns]
            if sum_first_cols:
                sum_rest = [c for c in summary_out.columns if c not in sum_first_cols]
                summary_out = summary_out[sum_first_cols + sum_rest].copy()
            time_summary_xlsx = safe_to_excel(
                summary_out.sort_values(
                    [
                        "Iteracao",
                        "_SENTIDO_rank",
                        "Load_kW",
                        "_DQ_rank",
                        "_SMP_rank",
                        "_ACT_rank",
                        "_ACT_TRANS_rank",
                        "_ECT_rank",
                        "_ECT_TRANS_rank",
                        "SourceFolder",
                        "BaseName",
                    ],
                    ascending=[True, True, True, True, True, True, True, True, True, True, True],
                    na_position="last",
                ).drop(
                    columns=[
                        "_DQ_rank",
                        "_SMP_rank",
                        "_ACT_rank",
                        "_ACT_TRANS_rank",
                        "_ECT_rank",
                        "_ECT_TRANS_rank",
                        "_SENTIDO_rank",
                    ]
                ).copy(),
                OUT_DIR / "lv_diagnostics_summay.xlsx",
            )
            print(f"[OK] Resumo geral de qualidade gerado: {time_summary_xlsx}")

        for source_folder, plot_dir, time_group in iter_source_plot_groups(lv_time_diag):
            source_label = source_folder if source_folder else "(raiz PROCESSAR)"
            print(f"[INFO] Gerando plots de delta T em {plot_dir} para {source_label}.")
            plot_time_delta_all_samples(time_group, plot_dir=plot_dir)
            plot_time_delta_by_file(time_group, plot_dir=plot_dir)
    else:
        print("[WARN] Coluna TIME nÃ£o encontrada ou invÃ¡lida; pulei o diagnÃ³stico de delta T.")

    trechos = compute_trechos_stats(lv_raw, instruments_df=instruments_df)
    ponto = compute_ponto_stats(trechos)

    lhv = load_lhv_lookup()
    kibox_agg = (
        kibox_aggregate(kibox_files)
        if kibox_files
        else pd.DataFrame(columns=["Load_kW", "DIES_pct", "BIOD_pct", "EtOH_pct", "H2O_pct"])
    )
    motec_ponto = pd.DataFrame(columns=["Load_kW", "DIES_pct", "BIOD_pct", "EtOH_pct", "H2O_pct"])
    if motec_files:
        motec_all: List[pd.DataFrame] = []
        for m in motec_files:
            try:
                df_i = read_motec_csv(m)
                if not df_i.empty:
                    motec_all.append(df_i)
            except Exception as e:
                print(f"[ERROR] Falha lendo MOTEC {m.path.name}: {e}")

        if motec_all:
            motec_raw = pd.concat(motec_all, ignore_index=True)
            motec_trechos = compute_motec_trechos_stats(motec_raw)
            motec_ponto = compute_motec_ponto_stats(motec_trechos)
            print(
                f"[INFO] MOTEC: {len(motec_files)} arquivo(s), "
                f"{len(motec_trechos)} trecho(s) valido(s), "
                f"{len(motec_ponto)} ponto(s) agregado(s)."
            )
        else:
            print("[WARN] Arquivos MOTEC encontrados, mas nenhum foi lido com sucesso.")

    out = build_final_table(ponto, lhv, kibox_agg, motec_ponto, mappings, instruments_df, reporting_df)

    out_xlsx = safe_to_excel(out, OUT_DIR / "lv_kpis_clean.xlsx")
    print(f"[OK] Excel gerado: {out_xlsx}")

    for source_folder, plot_dir, out_group in iter_source_plot_groups(out):
        source_label = source_folder if source_folder else "(raiz PROCESSAR)"
        print(f"[INFO] Gerando plots finais em {plot_dir} para {source_label}.")
        make_plots_from_config(out_group, plots_df, mappings=mappings, plot_dir=plot_dir)

    compare_groups = iter_compare_plot_groups(out, root=PLOTS_DIR)
    if compare_groups:
        for compare_key, plot_dir, cmp_group in compare_groups:
            series_vals = sorted(
                str(v).strip()
                for v in cmp_group.get("_COMPARE_SERIES", pd.Series([], dtype="object")).dropna().unique().tolist()
                if str(v).strip()
            )
            series_txt = ", ".join(series_vals) if series_vals else "origens desconhecidas"
            print(f"[INFO] Gerando plots de comparacao em {plot_dir} para '{compare_key}' ({series_txt}).")
            make_plots_from_config(
                cmp_group,
                plots_df,
                mappings=mappings,
                plot_dir=plot_dir,
                series_col="_COMPARE_SERIES",
            )
    else:
        print("[INFO] Nenhum par subida/descida detectado para gerar plots em compare/.")

    if kibox_files:
        print("[INFO] Kibox csv em raw/ detectado. (Histogramas KPEAK continuam fora do workflow por enquanto.)")
    else:
        print("[WARN] Sem Kibox csv em raw/.")


if __name__ == "__main__":
    main()
