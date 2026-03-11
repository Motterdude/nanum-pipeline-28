from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import locale
from pathlib import Path
import platform
import re
import subprocess
import sys
import time
from typing import Sequence

import numpy as np
import pandas as pd


# Python 3.12 on some Windows setups can decode `cmd /c ver` with the wrong code page.
_WIN_VER_OUTPUT_RE = re.compile(r"(?:([\w ]+) ([\w.]+) .* \[.* ([\d.]+)\])")


def _decode_windows_cmd_output(raw: bytes) -> str:
    candidates: list[str] = []
    locale_encoding = locale.getpreferredencoding(False)
    if locale_encoding:
        candidates.append(locale_encoding)
    getencoding = getattr(locale, "getencoding", None)
    if callable(getencoding):
        detected = getencoding()
        if detected and detected not in candidates:
            candidates.append(detected)
    for encoding in ("utf-8", "mbcs", "cp850", "cp437", "latin-1"):
        if encoding not in candidates:
            candidates.append(encoding)

    for encoding in candidates:
        try:
            return raw.decode(encoding)
        except (LookupError, UnicodeDecodeError):
            continue
    return raw.decode("latin-1", errors="replace")


def _fallback_syscmd_ver(
    system: str = "",
    release: str = "",
    version: str = "",
    supported_platforms: tuple[str, ...] = ("win32", "win16", "dos"),
) -> tuple[str, str, str]:
    if sys.platform not in supported_platforms:
        return system, release, version

    for cmd in ("ver", "command /c ver", "cmd /c ver"):
        try:
            raw = subprocess.check_output(
                cmd,
                stdin=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=False,
                shell=True,
            )
        except (OSError, subprocess.CalledProcessError):
            continue

        info = _decode_windows_cmd_output(raw).strip()
        if not info:
            continue

        for line in reversed([line.strip() for line in info.splitlines() if line.strip()]):
            match = _WIN_VER_OUTPUT_RE.search(line)
            if match is None:
                continue
            system, release, version = match.groups()
            if release.endswith("."):
                release = release[:-1]
            if version.endswith("."):
                version = version[:-1]
            return system, release, platform._norm_version(version)
        return system, release, version

    return system, release, version


def _patch_platform_syscmd_ver() -> None:
    if sys.platform != "win32":
        return

    original = getattr(platform, "_syscmd_ver", None)
    if original is None or getattr(platform, "_kibox_safe_syscmd_ver", False):
        return

    def _safe_syscmd_ver(
        system: str = "",
        release: str = "",
        version: str = "",
        supported_platforms: tuple[str, ...] = ("win32", "win16", "dos"),
    ) -> tuple[str, str, str]:
        try:
            return original(system, release, version, supported_platforms)
        except UnicodeDecodeError:
            return _fallback_syscmd_ver(system, release, version, supported_platforms)

    platform._syscmd_ver = _safe_syscmd_ver
    platform._kibox_safe_syscmd_ver = True


_patch_platform_syscmd_ver()

import pyqtgraph as pg
from PySide6 import QtCore, QtGui, QtWidgets

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CYCLE_BLOCK_SIZE = 30
DEFAULT_INPUT = BASE_DIR / "raw" / "PROCESSAR" / "kibox_input.csv"


PCYL_X_RANGE = (-40.0, 80.0)
Q1_X_RANGE = (-30.0, 90.0)
COMPARE_EXPORT_SIZE = (1600, 1200)

PEN_PCYL_SELECTED = (0, 245, 255)
PEN_Q1_SELECTED = (255, 110, 0)
PEN_BLOCK_MEAN = (255, 235, 59)
PEN_PMAX_CURVE = (100, 255, 0)
PEN_PMAX_CURSOR = (255, 0, 140)
PEN_PMAX_POINT = (255, 0, 140)
COMPARE_SLOT_COLORS = [
    (0, 245, 255),
    (255, 110, 0),
    (180, 255, 0),
]

REQUIRED_VIEWER_COLUMNS = ("CycleNumber", "CrankAngle_deg", "Volume_l", "PCYL_1", "Q_1")
VIEWER_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "CycleNumber": ("cyclenumber", "cycleno", "cyclenr", "cycle"),
    "CrankAngle_deg": ("crankangledegca", "crankangledeg", "crankangleca", "crankangle"),
    "Volume_l": ("volumel", "volume", "cylindervolume", "vol"),
    "PCYL_1": ("pcyl1bar", "pcyl1", "pcyl", "pcyl1abs"),
    "Q_1": ("q1jdegca", "q1jdeg", "q1"),
}


class UnsupportedTraceCsvError(ValueError):
    pass


@dataclass
class CurveData:
    x: np.ndarray
    y: np.ndarray


@dataclass
class BlockCurveData:
    x: np.ndarray
    y: np.ndarray
    label: str


@dataclass
class ViewerSeries:
    cycle_lookup: dict[int, CurveData]
    block_lookup: dict[int, BlockCurveData]
    y_label: str
    x_min: float
    x_max: float
    y_limits: tuple[float, float]


@dataclass
class PVSeries:
    cycle_lookup: dict[int, CurveData]
    block_lookup: dict[int, BlockCurveData]
    x_limits: tuple[float, float]
    y_limits: tuple[float, float]


@dataclass
class ViewerDataset:
    csv_path: Path
    pcyl_series: ViewerSeries
    pv_series: PVSeries
    q1_series: ViewerSeries
    pmax_cycles: np.ndarray
    pmax_values: np.ndarray
    pmax_map: dict[int, float]
    available_cycles: np.ndarray
    min_cycle: int
    max_cycle: int
    available_block_indices: np.ndarray
    min_block: int
    max_block: int


@dataclass
class CompareSelection:
    x: np.ndarray
    y: np.ndarray
    label: str
    summary: str
    mode: str
    cycle_reference: int
    selected_cycle: int
    block_label: str | None
    csv_path: Path


@dataclass
class CompareSlot:
    slot_index: int
    color: tuple[int, int, int]
    load_button: QtWidgets.QPushButton
    clear_button: QtWidgets.QPushButton
    file_label: QtWidgets.QLabel
    mode_combo: QtWidgets.QComboBox
    selector_label: QtWidgets.QLabel
    cycle_spin: QtWidgets.QSpinBox
    summary_label: QtWidgets.QLabel
    dataset: ViewerDataset | None = None
    pcyl_curve: pg.PlotDataItem | None = None
    q1_curve: pg.PlotDataItem | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fast Qt/PyQtGraph cycle-by-cycle KIBOX viewer."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Input KIBOX CSV file.")
    parser.add_argument(
        "--cycle-block-size",
        type=int,
        default=DEFAULT_CYCLE_BLOCK_SIZE,
        help="Number of cycles per block mean overlay.",
    )
    parser.add_argument(
        "--initial-cycle",
        type=int,
        default=1,
        help="Initial cycle shown when the viewer opens.",
    )
    parser.add_argument(
        "--hide-block-mean",
        action="store_true",
        help="Do not overlay the block mean curve.",
    )
    parser.add_argument(
        "--no-show",
        action="store_true",
        help="Prepare all data structures and exit without opening the GUI.",
    )
    return parser.parse_args()


def _normalize_cols(cols: Sequence[object]) -> list[str]:
    return [str(c).replace("\ufeff", "").strip() for c in cols]


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
    return re.sub(r"__+", "_", s)


def _coalesce_equivalent_columns(df: pd.DataFrame, context: str = "") -> pd.DataFrame:
    if df is None or df.empty:
        return df

    merged: dict[str, pd.Series] = {}
    sources: dict[str, list[str]] = {}
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


def _canon_header(x: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(x).replace("\ufeff", "").strip().lower())


def _decode_text_with_fallbacks(raw: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", errors="replace")


def _read_text_sample(csv_path: Path, max_bytes: int = 262_144) -> str:
    with csv_path.open("rb") as fh:
        raw = fh.read(max_bytes)
    return _decode_text_with_fallbacks(raw)


def _coerce_numeric_series(s: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce")

    x = s.astype(str).str.replace("\ufeff", "", regex=False).str.strip()
    x = x.str.replace("\u00A0", " ", regex=False).str.replace(" ", "", regex=False)
    x = x.str.replace(r"[^0-9,.\-+]+", "", regex=True)

    def _fix_num(v: object) -> str:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return ""
        v = str(v)
        if not v:
            return ""
        if "," in v and "." in v:
            if v.rfind(",") > v.rfind("."):
                return v.replace(".", "").replace(",", ".")
            return v.replace(",", "")
        if "," in v:
            return v.replace(",", ".")
        return v

    fixed = x.map(_fix_num)
    return pd.to_numeric(fixed, errors="coerce")


def _resolve_required_columns(columns: Sequence[object]) -> dict[str, str]:
    resolved: dict[str, str] = {}
    canon_map: dict[str, str] = {}
    for raw in _normalize_cols(columns):
        canon = _canon_header(raw)
        if canon and canon not in canon_map:
            canon_map[canon] = raw

    for target, aliases in VIEWER_COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in canon_map:
                resolved[target] = canon_map[alias]
                break
        if target in resolved:
            continue
        for canon, raw in canon_map.items():
            if any(canon.startswith(alias) or alias.startswith(canon) for alias in aliases):
                resolved[target] = raw
                break
    return resolved


def _detect_kibox_layout(text: str) -> tuple[str, int]:
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        raise ValueError("Arquivo CSV vazio.")

    best: tuple[int, int, int, str, int] | None = None
    for delim in ("\t", ";", ",", "|"):
        for row_idx, line in enumerate(lines[:80]):
            cells = [cell.strip() for cell in line.split(delim)]
            if len(cells) < 2:
                continue
            resolved = _resolve_required_columns(cells)
            alpha_cells = sum(1 for cell in cells if any(ch.isalpha() for ch in cell))
            score = (len(resolved), alpha_cells, len(cells), delim, row_idx)
            if best is None or score > best:
                best = score

    if best is None:
        raise ValueError("Nao consegui detectar o formato do CSV.")
    return best[3], best[4]


def _read_kibox_csv_raw(csv_path: Path) -> pd.DataFrame:
    text = _read_text_sample(csv_path)
    delimiter, header_row = _detect_kibox_layout(text)

    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            df = pd.read_csv(
                csv_path,
                sep=delimiter,
                engine="python",
                encoding=encoding,
                skiprows=header_row,
                dtype=str,
            )
            break
        except UnicodeDecodeError as exc:
            last_error = exc
    else:
        raise ValueError(f"Nao consegui ler o CSV com as codificacoes tentadas: {csv_path}") from last_error

    df.columns = _normalize_cols(list(df.columns))
    df = df.loc[:, ~pd.Series(df.columns).astype(str).str.startswith("Unnamed").values].copy()
    df = _coalesce_equivalent_columns(df, context=csv_path.name)
    return df


def _try_fast_load_cycle_dataframe(csv_path: Path) -> pd.DataFrame | None:
    text = _read_text_sample(csv_path)
    delimiter, header_row = _detect_kibox_layout(text)
    if header_row != 0:
        return None

    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return None
    header_cells = _normalize_cols(lines[header_row].split(delimiter))
    resolved = _resolve_required_columns(header_cells)
    if any(col not in resolved for col in REQUIRED_VIEWER_COLUMNS):
        return None

    usecols = [
        resolved["CycleNumber"],
        resolved["CrankAngle_deg"],
        resolved["Volume_l"],
        resolved["PCYL_1"],
        resolved["Q_1"],
    ]
    skiprows = [1] if len(lines) > 1 and any(ch.isalpha() for ch in lines[1]) else None
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            df = pd.read_csv(
                csv_path,
                sep=delimiter,
                decimal=",",
                encoding=encoding,
                skiprows=skiprows,
                usecols=usecols,
                memory_map=True,
            )
            df = df.rename(
                columns={
                    resolved["CycleNumber"]: "CycleNumber",
                    resolved["CrankAngle_deg"]: "CrankAngle_deg",
                    resolved["Volume_l"]: "Volume_l",
                    resolved["PCYL_1"]: "PCYL_1",
                    resolved["Q_1"]: "Q_1",
                }
            )
            df["CycleNumber"] = pd.to_numeric(df["CycleNumber"], errors="coerce").ffill()
            df["CrankAngle_deg"] = pd.to_numeric(df["CrankAngle_deg"], errors="coerce").round(1)
            df["Volume_l"] = pd.to_numeric(df["Volume_l"], errors="coerce")
            df["PCYL_1"] = pd.to_numeric(df["PCYL_1"], errors="coerce")
            df["Q_1"] = pd.to_numeric(df["Q_1"], errors="coerce")
            df = df.dropna(subset=["CycleNumber", "CrankAngle_deg"])
            df["CycleNumber"] = df["CycleNumber"].astype("int64")
            return df
        except (UnicodeDecodeError, ValueError) as exc:
            last_error = exc
            continue
    if last_error is not None:
        raise last_error
    return None


def load_cycle_dataframe(csv_path: Path) -> pd.DataFrame:
    fast_df = _try_fast_load_cycle_dataframe(csv_path)
    if fast_df is not None:
        return fast_df

    raw_df = _read_kibox_csv_raw(csv_path)
    resolved = _resolve_required_columns(raw_df.columns)
    missing = [col for col in REQUIRED_VIEWER_COLUMNS if col not in resolved]
    if missing:
        found = ", ".join(list(raw_df.columns)[:12]) or "(nenhuma coluna)"
        raise UnsupportedTraceCsvError(
            "CSV incompativel com o viewer. "
            "Esperado um export KIBOX com colunas equivalentes a "
            "'Cycle number', 'Crank angle', 'Volume', 'PCYL_1' e 'Q_1'. "
            f"Colunas encontradas: {found}"
        )

    df = raw_df.rename(columns={raw: canonical for canonical, raw in resolved.items()})
    df = df[list(REQUIRED_VIEWER_COLUMNS)].copy()
    df["CycleNumber"] = _coerce_numeric_series(df["CycleNumber"]).ffill()
    df["CrankAngle_deg"] = _coerce_numeric_series(df["CrankAngle_deg"]).round(1)
    df["Volume_l"] = _coerce_numeric_series(df["Volume_l"])
    df["PCYL_1"] = _coerce_numeric_series(df["PCYL_1"])
    df["Q_1"] = _coerce_numeric_series(df["Q_1"])
    df = df.dropna(subset=["CycleNumber", "CrankAngle_deg"])
    df["CycleNumber"] = df["CycleNumber"].astype("int64")
    return df


def _prompt_input_csv(initial_target: Path) -> Path | None:
    app = QtWidgets.QApplication.instance() or QtWidgets.QApplication([])

    if initial_target.exists() and initial_target.is_dir():
        start_dir = initial_target
    elif initial_target.parent.exists():
        start_dir = initial_target.parent
    else:
        start_dir = BASE_DIR

    selected, _ = QtWidgets.QFileDialog.getOpenFileName(
        None,
        "Selecione o CSV do KIBOX",
        str(start_dir),
        "CSV Files (*.csv);;All Files (*.*)",
    )
    if not selected:
        return None
    return Path(selected).resolve()


def build_per_cycle_means(df: pd.DataFrame) -> pd.DataFrame:
    # KIBOX files used here already contain one row per cycle/crank-angle pair.
    # A global groupby on both keys only burns memory on large files.
    per_cycle = df[["CycleNumber", "CrankAngle_deg", "Volume_l", "PCYL_1", "Q_1"]].sort_values(
        ["CycleNumber", "CrankAngle_deg"]
    )
    per_cycle["CycleNumber"] = per_cycle["CycleNumber"].astype(np.int32)
    per_cycle["CrankAngle_deg"] = per_cycle["CrankAngle_deg"].astype(np.float32)
    per_cycle["Volume_l"] = pd.to_numeric(per_cycle["Volume_l"], errors="coerce", downcast="float")
    per_cycle["PCYL_1"] = pd.to_numeric(per_cycle["PCYL_1"], errors="coerce", downcast="float")
    per_cycle["Q_1"] = pd.to_numeric(per_cycle["Q_1"], errors="coerce", downcast="float")
    return per_cycle


def build_pmax_series(df: pd.DataFrame) -> pd.DataFrame:
    pmax = (
        df.dropna(subset=["PCYL_1"])
        .groupby("CycleNumber", as_index=False)["PCYL_1"]
        .max()
        .rename(columns={"PCYL_1": "PMAX_bar"})
        .sort_values("CycleNumber")
    )
    pmax["CycleNumber"] = pmax["CycleNumber"].astype(np.int32)
    pmax["PMAX_bar"] = pd.to_numeric(pmax["PMAX_bar"], errors="coerce", downcast="float")
    return pmax


def _build_cycle_lookup(per_cycle: pd.DataFrame, value_col: str, x_min: float, x_max: float) -> dict[int, CurveData]:
    filtered = per_cycle[
        (per_cycle["CrankAngle_deg"] >= x_min)
        & (per_cycle["CrankAngle_deg"] <= x_max)
        & per_cycle[value_col].notna()
    ][["CycleNumber", "CrankAngle_deg", value_col]]
    out: dict[int, CurveData] = {}
    for cycle, group in filtered.groupby("CycleNumber", sort=True):
        out[int(cycle)] = CurveData(
            x=group["CrankAngle_deg"].to_numpy(dtype=np.float32, copy=True),
            y=group[value_col].to_numpy(dtype=np.float32, copy=True),
        )
    return out


def _build_block_lookup(
    per_cycle: pd.DataFrame,
    value_col: str,
    cycle_block_size: int,
    x_min: float,
    x_max: float,
) -> dict[int, BlockCurveData]:
    filtered = per_cycle[
        (per_cycle["CrankAngle_deg"] >= x_min)
        & (per_cycle["CrankAngle_deg"] <= x_max)
        & per_cycle[value_col].notna()
    ][["CycleNumber", "CrankAngle_deg", value_col]].copy()
    if filtered.empty:
        return {}

    filtered["CycleBlockIndex"] = ((filtered["CycleNumber"] - 1) // cycle_block_size) + 1
    block_curve = (
        filtered.groupby(["CycleBlockIndex", "CrankAngle_deg"], as_index=False, sort=False)[value_col]
        .mean()
        .sort_values(["CycleBlockIndex", "CrankAngle_deg"])
    )
    max_cycle = int(filtered["CycleNumber"].max())
    block_curve["CycleBlockStart"] = ((block_curve["CycleBlockIndex"] - 1) * cycle_block_size) + 1
    block_curve["CycleBlockEnd"] = (block_curve["CycleBlockStart"] + cycle_block_size - 1).clip(upper=max_cycle)
    block_curve["CycleBlockLabel"] = (
        block_curve["CycleBlockStart"].astype(str) + "-" + block_curve["CycleBlockEnd"].astype(str)
    )

    out: dict[int, BlockCurveData] = {}
    for block_index, group in block_curve.groupby("CycleBlockIndex", sort=True):
        out[int(block_index)] = BlockCurveData(
            x=group["CrankAngle_deg"].to_numpy(dtype=np.float32, copy=True),
            y=group[value_col].to_numpy(dtype=np.float32, copy=True),
            label=str(group["CycleBlockLabel"].iloc[0]),
        )
    return out


def _compute_y_limits(cycle_lookup: dict[int, CurveData], block_lookup: dict[int, BlockCurveData]) -> tuple[float, float]:
    arrays: list[np.ndarray] = []
    for d in cycle_lookup.values():
        if d.y.size:
            arrays.append(d.y.astype(float, copy=False))
    for d in block_lookup.values():
        if d.y.size:
            arrays.append(d.y.astype(float, copy=False))
    if not arrays:
        return (0.0, 1.0)

    all_values = np.concatenate(arrays)
    ymin = float(np.nanmin(all_values))
    ymax = float(np.nanmax(all_values))
    if np.isclose(ymin, ymax):
        pad = max(abs(ymin) * 0.05, 1.0)
        return (ymin - pad, ymax + pad)
    pad = (ymax - ymin) * 0.05
    return (ymin - pad, ymax + pad)


def _compute_x_limits(cycle_lookup: dict[int, CurveData], block_lookup: dict[int, BlockCurveData]) -> tuple[float, float]:
    arrays: list[np.ndarray] = []
    for d in cycle_lookup.values():
        if d.x.size:
            arrays.append(d.x.astype(float, copy=False))
    for d in block_lookup.values():
        if d.x.size:
            arrays.append(d.x.astype(float, copy=False))
    if not arrays:
        return (1e-4, 1.0)

    all_values = np.concatenate(arrays)
    valid = all_values[np.isfinite(all_values) & (all_values > 0.0)]
    if valid.size == 0:
        return (1e-4, 1.0)

    xmin = float(np.nanmin(valid))
    xmax = float(np.nanmax(valid))
    if np.isclose(xmin, xmax):
        return (max(xmin / 1.2, 1e-6), xmax * 1.2)
    pad = (xmax - xmin) * 0.03
    return (max(xmin - pad, 1e-6), xmax + pad)


def _compute_positive_y_limits(
    cycle_lookup: dict[int, CurveData],
    block_lookup: dict[int, BlockCurveData],
) -> tuple[float, float]:
    arrays: list[np.ndarray] = []
    for d in cycle_lookup.values():
        if d.y.size:
            arrays.append(d.y.astype(float, copy=False))
    for d in block_lookup.values():
        if d.y.size:
            arrays.append(d.y.astype(float, copy=False))
    if not arrays:
        return (0.1, 1.0)

    all_values = np.concatenate(arrays)
    valid = all_values[np.isfinite(all_values) & (all_values > 0.0)]
    if valid.size == 0:
        return (0.1, 1.0)

    ymin = float(np.nanmin(valid))
    ymax = float(np.nanmax(valid))
    if np.isclose(ymin, ymax):
        return (max(ymin / 1.2, 1e-3), ymax * 1.2)
    return (max(ymin / 1.1, 1e-3), ymax * 1.1)


def build_pv_series(per_cycle: pd.DataFrame, *, cycle_block_size: int) -> PVSeries:
    filtered = per_cycle[
        per_cycle["Volume_l"].notna()
        & (per_cycle["Volume_l"] > 0.0)
        & per_cycle["PCYL_1"].notna()
        & (per_cycle["PCYL_1"] > 0.0)
    ][["CycleNumber", "CrankAngle_deg", "Volume_l", "PCYL_1"]].copy()
    if filtered.empty:
        return PVSeries(cycle_lookup={}, block_lookup={}, x_limits=(1e-4, 1.0), y_limits=(0.1, 1.0))

    filtered = filtered.sort_values(["CycleNumber", "CrankAngle_deg"])

    cycle_lookup: dict[int, CurveData] = {}
    for cycle, group in filtered.groupby("CycleNumber", sort=True):
        cycle_lookup[int(cycle)] = CurveData(
            x=group["Volume_l"].to_numpy(dtype=np.float32, copy=True),
            y=group["PCYL_1"].to_numpy(dtype=np.float32, copy=True),
        )

    filtered["CycleBlockIndex"] = ((filtered["CycleNumber"] - 1) // cycle_block_size) + 1
    block_curve = (
        filtered.groupby(["CycleBlockIndex", "CrankAngle_deg"], as_index=False, sort=False)[["Volume_l", "PCYL_1"]]
        .mean()
        .sort_values(["CycleBlockIndex", "CrankAngle_deg"])
    )
    max_cycle = int(filtered["CycleNumber"].max())
    block_curve["CycleBlockStart"] = ((block_curve["CycleBlockIndex"] - 1) * cycle_block_size) + 1
    block_curve["CycleBlockEnd"] = (block_curve["CycleBlockStart"] + cycle_block_size - 1).clip(upper=max_cycle)
    block_curve["CycleBlockLabel"] = (
        block_curve["CycleBlockStart"].astype(str) + "-" + block_curve["CycleBlockEnd"].astype(str)
    )

    block_lookup: dict[int, BlockCurveData] = {}
    for block_index, group in block_curve.groupby("CycleBlockIndex", sort=True):
        block_lookup[int(block_index)] = BlockCurveData(
            x=group["Volume_l"].to_numpy(dtype=np.float32, copy=True),
            y=group["PCYL_1"].to_numpy(dtype=np.float32, copy=True),
            label=str(group["CycleBlockLabel"].iloc[0]),
        )

    return PVSeries(
        cycle_lookup=cycle_lookup,
        block_lookup=block_lookup,
        x_limits=_compute_x_limits(cycle_lookup, block_lookup),
        y_limits=_compute_positive_y_limits(cycle_lookup, block_lookup),
    )


def _log10_range(limits: tuple[float, float]) -> tuple[float, float]:
    lo = max(float(limits[0]), 1e-12)
    hi = max(float(limits[1]), lo * 1.0001)
    return (float(np.log10(lo)), float(np.log10(hi)))


def build_viewer_series(
    per_cycle: pd.DataFrame,
    *,
    value_col: str,
    y_label: str,
    x_range: tuple[float, float],
    cycle_block_size: int,
) -> ViewerSeries:
    x_min, x_max = x_range
    cycle_lookup = _build_cycle_lookup(per_cycle, value_col, x_min, x_max)
    block_lookup = _build_block_lookup(per_cycle, value_col, cycle_block_size, x_min, x_max)
    y_limits = _compute_y_limits(cycle_lookup, block_lookup)
    return ViewerSeries(
        cycle_lookup=cycle_lookup,
        block_lookup=block_lookup,
        y_label=y_label,
        x_min=x_min,
        x_max=x_max,
        y_limits=y_limits,
    )


def _nearest_available_cycle(cycle: int, available_cycles: np.ndarray) -> int:
    idx = int(np.argmin(np.abs(available_cycles - cycle)))
    return int(available_cycles[idx])


def prepare_viewer_dataset(csv_path: Path, cycle_block_size: int) -> ViewerDataset:
    df = load_cycle_dataframe(csv_path)
    df["CycleNumber"] = df["CycleNumber"].astype(np.int32)
    df["CrankAngle_deg"] = df["CrankAngle_deg"].astype(np.float32)
    df["Volume_l"] = pd.to_numeric(df["Volume_l"], errors="coerce", downcast="float")
    df["PCYL_1"] = pd.to_numeric(df["PCYL_1"], errors="coerce", downcast="float")
    df["Q_1"] = pd.to_numeric(df["Q_1"], errors="coerce", downcast="float")

    per_cycle = build_per_cycle_means(df)
    pmax_series = build_pmax_series(df)
    pcyl_series = build_viewer_series(
        per_cycle,
        value_col="PCYL_1",
        y_label="P_CYL (bar)",
        x_range=PCYL_X_RANGE,
        cycle_block_size=cycle_block_size,
    )
    pv_series = build_pv_series(
        per_cycle,
        cycle_block_size=cycle_block_size,
    )
    q1_series = build_viewer_series(
        per_cycle,
        value_col="Q_1",
        y_label="Q_1 (J/deg CA)",
        x_range=Q1_X_RANGE,
        cycle_block_size=cycle_block_size,
    )
    available_cycles = np.asarray(
        sorted(set(pcyl_series.cycle_lookup) | set(q1_series.cycle_lookup)),
        dtype=np.int32,
    )
    if available_cycles.size == 0:
        raise ValueError(f"No cycles available in {csv_path}.")

    pmax_cycles = pmax_series["CycleNumber"].to_numpy(dtype=np.int32, copy=True)
    pmax_values = pmax_series["PMAX_bar"].to_numpy(dtype=np.float32, copy=True)
    pmax_map = dict(zip(pmax_cycles.tolist(), pmax_values.tolist()))
    available_block_indices = np.asarray(
        sorted(set(pcyl_series.block_lookup) | set(q1_series.block_lookup)),
        dtype=np.int32,
    )
    if available_block_indices.size == 0:
        available_block_indices = np.asarray([1], dtype=np.int32)
    return ViewerDataset(
        csv_path=csv_path,
        pcyl_series=pcyl_series,
        pv_series=pv_series,
        q1_series=q1_series,
        pmax_cycles=pmax_cycles,
        pmax_values=pmax_values,
        pmax_map=pmax_map,
        available_cycles=available_cycles,
        min_cycle=int(available_cycles[0]),
        max_cycle=int(available_cycles[-1]),
        available_block_indices=available_block_indices,
        min_block=int(available_block_indices[0]),
        max_block=int(available_block_indices[-1]),
    )


class FastCycleViewer(QtWidgets.QWidget):
    def __init__(
        self,
        *,
        dataset: ViewerDataset,
        cycle_block_size: int,
        initial_cycle: int,
        show_block_mean: bool,
    ) -> None:
        super().__init__()
        self.cycle_block_size = cycle_block_size
        self.show_block_mean = show_block_mean
        self.current_cycle: int | None = None
        self._syncing = False
        self._syncing_viewer_x = False
        self.dataset_cache: dict[Path, ViewerDataset] = {dataset.csv_path.resolve(): dataset}
        self.dataset = dataset
        self.csv_path = dataset.csv_path
        self.pcyl_series = dataset.pcyl_series
        self.pv_series = dataset.pv_series
        self.q1_series = dataset.q1_series
        self.pmax_cycles = dataset.pmax_cycles
        self.pmax_values = dataset.pmax_values
        self.pmax_map = dataset.pmax_map
        self.available_cycles = dataset.available_cycles
        self.min_cycle = dataset.min_cycle
        self.max_cycle = dataset.max_cycle
        self.cycle_to_index = {int(c): i for i, c in enumerate(self.available_cycles.tolist())}
        self.initial_cycle = _nearest_available_cycle(initial_cycle, self.available_cycles)
        self.compare_slots: list[CompareSlot] = []

        self._setup_ui()
        self._configure_plots()
        self._configure_compare_plots()
        self.update_cycle(self.initial_cycle)
        self._assign_compare_slot_dataset(0, self.dataset, default_cycle=self.current_cycle)
        self._update_compare_plots()

    def _setup_ui(self) -> None:
        self.setWindowTitle(f"Fast KIBOX cycle viewer - {self.csv_path.name}")
        self.resize(1650, 1050)

        layout = QtWidgets.QVBoxLayout(self)
        self.tabs = QtWidgets.QTabWidget()
        layout.addWidget(self.tabs, stretch=1)

        self.viewer_tab = QtWidgets.QWidget()
        self.compare_tab = QtWidgets.QWidget()
        self.tabs.addTab(self.viewer_tab, "Viewer")
        self.tabs.addTab(self.compare_tab, "Compare")

        viewer_layout = QtWidgets.QVBoxLayout(self.viewer_tab)
        self.graphics = pg.GraphicsLayoutWidget()
        viewer_layout.addWidget(self.graphics, stretch=1)

        self.pcyl_plot = self.graphics.addPlot(row=0, col=0)
        self.pv_plot = self.graphics.addPlot(row=0, col=1)
        self.q1_plot = self.graphics.addPlot(row=1, col=0)
        self.pmax_plot = self.graphics.addPlot(row=1, col=1)

        control_layout = QtWidgets.QHBoxLayout()
        viewer_layout.addLayout(control_layout)

        self.open_button = QtWidgets.QPushButton("Open CSV")
        self.open_button.clicked.connect(self._open_csv_dialog)
        control_layout.addWidget(self.open_button)

        self.file_label = QtWidgets.QLabel(self.csv_path.name)
        self.file_label.setMinimumWidth(320)
        control_layout.addWidget(self.file_label)

        control_layout.addWidget(QtWidgets.QLabel("Cycle"))
        self.slider = QtWidgets.QSlider(QtCore.Qt.Orientation.Horizontal)
        self.slider.setMinimum(self.min_cycle)
        self.slider.setMaximum(self.max_cycle)
        self.slider.setSingleStep(1)
        self.slider.setPageStep(1)
        self.slider.setTracking(True)
        control_layout.addWidget(self.slider, stretch=1)

        control_layout.addWidget(QtWidgets.QLabel("Go to"))
        self.cycle_edit = QtWidgets.QLineEdit(str(self.initial_cycle))
        self.cycle_edit.setMaximumWidth(100)
        control_layout.addWidget(self.cycle_edit)

        hint = QtWidgets.QLabel("Left/Right: step one cycle")
        hint.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignVCenter)
        control_layout.addWidget(hint)

        self.slider.valueChanged.connect(self._on_slider_changed)
        self.cycle_edit.returnPressed.connect(self._on_cycle_edit_submitted)

        compare_layout = QtWidgets.QVBoxLayout(self.compare_tab)
        self.compare_graphics = pg.GraphicsLayoutWidget()
        compare_layout.addWidget(self.compare_graphics, stretch=1)

        self.compare_pcyl_plot = self.compare_graphics.addPlot(row=0, col=0)
        self.compare_q1_plot = self.compare_graphics.addPlot(row=0, col=1)

        compare_action_row = QtWidgets.QHBoxLayout()
        compare_layout.addLayout(compare_action_row)

        self.copy_current_button = QtWidgets.QPushButton("Copy Current to Slot 1")
        self.copy_current_button.clicked.connect(self._copy_current_to_slot_one)
        compare_action_row.addWidget(self.copy_current_button)

        self.export_compare_button = QtWidgets.QPushButton("Export Compare")
        self.export_compare_button.clicked.connect(self._export_compare_plots)
        compare_action_row.addWidget(self.export_compare_button)

        compare_hint = QtWidgets.QLabel(
            "Each slot can show a single cycle or the block mean referenced by a cycle."
        )
        compare_action_row.addWidget(compare_hint)
        compare_action_row.addStretch(1)

        for slot_index, color in enumerate(COMPARE_SLOT_COLORS, start=1):
            slot_row = QtWidgets.QHBoxLayout()
            compare_layout.addLayout(slot_row)

            slot_row.addWidget(QtWidgets.QLabel(f"Trace {slot_index}"))

            load_button = QtWidgets.QPushButton("Load CSV")
            clear_button = QtWidgets.QPushButton("Clear")
            file_label = QtWidgets.QLabel("No file loaded")
            file_label.setMinimumWidth(320)
            mode_combo = QtWidgets.QComboBox()
            mode_combo.addItems(["Cycle", "Block mean"])
            mode_combo.setEnabled(False)
            selector_label = QtWidgets.QLabel("Cycle ref")
            cycle_spin = QtWidgets.QSpinBox()
            cycle_spin.setEnabled(False)
            cycle_spin.setRange(1, 1)
            cycle_spin.setMaximumWidth(120)
            summary_label = QtWidgets.QLabel("Empty")
            summary_label.setMinimumWidth(220)

            slot_row.addWidget(load_button)
            slot_row.addWidget(clear_button)
            slot_row.addWidget(file_label, stretch=1)
            slot_row.addWidget(QtWidgets.QLabel("Mode"))
            slot_row.addWidget(mode_combo)
            slot_row.addWidget(selector_label)
            slot_row.addWidget(cycle_spin)
            slot_row.addWidget(summary_label)

            slot = CompareSlot(
                slot_index=slot_index,
                color=color,
                load_button=load_button,
                clear_button=clear_button,
                file_label=file_label,
                mode_combo=mode_combo,
                selector_label=selector_label,
                cycle_spin=cycle_spin,
                summary_label=summary_label,
            )
            clear_button.setEnabled(False)
            load_button.clicked.connect(lambda _checked=False, idx=slot_index - 1: self._open_compare_csv_dialog(idx))
            clear_button.clicked.connect(lambda _checked=False, idx=slot_index - 1: self._clear_compare_slot(idx))
            mode_combo.currentIndexChanged.connect(
                lambda _value=0, idx=slot_index - 1: self._on_compare_mode_changed(idx)
            )
            cycle_spin.valueChanged.connect(lambda _value=0: self._update_compare_plots())
            self.compare_slots.append(slot)

    def _configure_plots(self) -> None:
        pg.setConfigOptions(antialias=True, useOpenGL=True)

        self.pcyl_plot.showGrid(x=True, y=True, alpha=0.25)
        self.pv_plot.showGrid(x=True, y=True, alpha=0.25)
        self.q1_plot.showGrid(x=True, y=True, alpha=0.25)
        self.pmax_plot.showGrid(x=True, y=True, alpha=0.25)

        self.pcyl_plot.setLabel("bottom", "Crank angle", units="deg CA")
        self.pcyl_plot.setLabel("left", self.pcyl_series.y_label)
        self.pv_plot.setLabel("bottom", "Volume", units="l")
        self.pv_plot.setLabel("left", "P_CYL (bar)")
        self.q1_plot.setLabel("bottom", "Crank angle", units="deg CA")
        self.q1_plot.setLabel("left", self.q1_series.y_label)
        self.pmax_plot.setLabel("bottom", "Cycle")
        self.pmax_plot.setLabel("left", "PMAX (bar)")

        self.pv_plot.setLogMode(x=True, y=True)
        self.pcyl_plot.setXRange(self.pcyl_series.x_min, self.pcyl_series.x_max, padding=0.0)
        self.q1_plot.setXRange(self.q1_series.x_min, self.q1_series.x_max, padding=0.0)
        self.pcyl_plot.setYRange(*self.pcyl_series.y_limits, padding=0.0)
        self.q1_plot.setYRange(*self.q1_series.y_limits, padding=0.0)
        self.pv_plot.setXRange(*_log10_range(self.pv_series.x_limits), padding=0.0)
        self.pv_plot.setYRange(*_log10_range(self.pv_series.y_limits), padding=0.0)

        pmax_min = float(np.nanmin(self.pmax_values))
        pmax_max = float(np.nanmax(self.pmax_values))
        pmax_pad = max((pmax_max - pmax_min) * 0.05, 1.0)
        self.pmax_plot.setXRange(float(self.min_cycle), float(self.max_cycle), padding=0.0)
        self.pmax_plot.setYRange(pmax_min - pmax_pad, pmax_max + pmax_pad, padding=0.0)

        pcyl_pen = pg.mkPen(color=PEN_PCYL_SELECTED, width=1.6)
        q1_pen = pg.mkPen(color=PEN_Q1_SELECTED, width=1.6)
        block_pen = pg.mkPen(color=PEN_BLOCK_MEAN, width=1.2, style=QtCore.Qt.PenStyle.DashLine)
        pmax_pen = pg.mkPen(color=PEN_PMAX_CURVE, width=1.3)
        cursor_pen = pg.mkPen(color=PEN_PMAX_CURSOR, width=1.2)

        self.pcyl_curve = self.pcyl_plot.plot(pen=pcyl_pen, name="Selected cycle")
        self.pcyl_block_curve = self.pcyl_plot.plot(pen=block_pen, name="Block mean")
        self.pv_curve = self.pv_plot.plot(pen=pcyl_pen, name="Selected cycle")
        self.pv_block_curve = self.pv_plot.plot(pen=block_pen, name="Block mean")
        self.q1_curve = self.q1_plot.plot(pen=q1_pen, name="Selected cycle")
        self.q1_block_curve = self.q1_plot.plot(pen=block_pen, name="Block mean")
        self.pmax_curve = self.pmax_plot.plot(self.pmax_cycles, self.pmax_values, pen=pmax_pen, name="PMAX per cycle")
        self.pmax_cursor = pg.InfiniteLine(pos=float(self.initial_cycle), angle=90, pen=cursor_pen)
        self.pmax_plot.addItem(self.pmax_cursor)
        self.pmax_point = pg.ScatterPlotItem(size=6, brush=pg.mkBrush(*PEN_PMAX_POINT), pen=pg.mkPen(None))
        self.pmax_plot.addItem(self.pmax_point)

        if not self.show_block_mean:
            self.pcyl_block_curve.hide()
            self.pv_block_curve.hide()
            self.q1_block_curve.hide()

        legend1 = self.pcyl_plot.addLegend(offset=(10, 10))
        legend1.addItem(self.pcyl_curve, "Selected cycle")
        legend1.addItem(self.pcyl_block_curve, "Block mean")

        legend2 = self.pv_plot.addLegend(offset=(10, 10))
        legend2.addItem(self.pv_curve, "Selected cycle")
        legend2.addItem(self.pv_block_curve, "Block mean")

        legend3 = self.q1_plot.addLegend(offset=(10, 10))
        legend3.addItem(self.q1_curve, "Selected cycle")
        legend3.addItem(self.q1_block_curve, "Block mean")

        legend4 = self.pmax_plot.addLegend(offset=(10, 10))
        legend4.addItem(self.pmax_curve, "PMAX per cycle")

        # Keep crank-angle plots aligned during interactive pan/zoom.
        self.pcyl_plot.getViewBox().sigXRangeChanged.connect(
            lambda _vb, xr: self._sync_viewer_crank_x("pcyl", xr)
        )
        self.q1_plot.getViewBox().sigXRangeChanged.connect(
            lambda _vb, xr: self._sync_viewer_crank_x("q1", xr)
        )

    def _sync_viewer_crank_x(self, source: str, x_range: tuple[float, float]) -> None:
        if self._syncing_viewer_x:
            return
        if x_range is None or len(x_range) != 2:
            return

        x_min, x_max = float(x_range[0]), float(x_range[1])
        self._syncing_viewer_x = True
        try:
            if source != "pcyl":
                self.pcyl_plot.setXRange(x_min, x_max, padding=0.0)
            if source != "q1":
                self.q1_plot.setXRange(x_min, x_max, padding=0.0)
        finally:
            self._syncing_viewer_x = False

    def _configure_compare_plots(self) -> None:
        self.compare_pcyl_plot.showGrid(x=True, y=True, alpha=0.25)
        self.compare_q1_plot.showGrid(x=True, y=True, alpha=0.25)

        self.compare_pcyl_plot.setLabel("bottom", "Crank angle", units="deg CA")
        self.compare_pcyl_plot.setLabel("left", "P_CYL (bar)")
        self.compare_q1_plot.setLabel("bottom", "Crank angle", units="deg CA")
        self.compare_q1_plot.setLabel("left", "Q_1 (J/deg CA)")

        self.compare_pcyl_plot.setXRange(PCYL_X_RANGE[0], PCYL_X_RANGE[1], padding=0.0)
        self.compare_q1_plot.setXRange(Q1_X_RANGE[0], Q1_X_RANGE[1], padding=0.0)
        self.compare_pcyl_plot.setYRange(*self.pcyl_series.y_limits, padding=0.0)
        self.compare_q1_plot.setYRange(*self.q1_series.y_limits, padding=0.0)

        self.compare_pcyl_legend = self.compare_pcyl_plot.addLegend(offset=(10, 10))
        self.compare_q1_legend = self.compare_q1_plot.addLegend(offset=(10, 10))

        for slot in self.compare_slots:
            pen = pg.mkPen(color=slot.color, width=1.5)
            slot.pcyl_curve = self.compare_pcyl_plot.plot(pen=pen)
            slot.q1_curve = self.compare_q1_plot.plot(pen=pen)
            slot.pcyl_curve.hide()
            slot.q1_curve.hide()

    def _load_dataset(self, csv_path: Path) -> ViewerDataset:
        resolved = csv_path.resolve()
        cached = self.dataset_cache.get(resolved)
        if cached is not None:
            return cached
        dataset = prepare_viewer_dataset(resolved, self.cycle_block_size)
        self.dataset_cache[resolved] = dataset
        return dataset

    def _assign_compare_slot_dataset(
        self,
        slot_index: int,
        dataset: ViewerDataset,
        *,
        default_cycle: int | None = None,
    ) -> None:
        slot = self.compare_slots[slot_index]
        slot.dataset = dataset
        slot.file_label.setText(dataset.csv_path.name)
        slot.mode_combo.setEnabled(True)
        slot.cycle_spin.setEnabled(True)
        slot.clear_button.setEnabled(True)

        selected_cycle = default_cycle if default_cycle is not None else dataset.min_cycle
        selected_cycle = _nearest_available_cycle(int(selected_cycle), dataset.available_cycles)
        self._configure_compare_selector(slot, selected_cycle=selected_cycle)
        slot.summary_label.setText(f"Cycle {selected_cycle}")

    def _configure_compare_selector(self, slot: CompareSlot, *, selected_cycle: int | None = None) -> None:
        if slot.dataset is None:
            return

        mode = slot.mode_combo.currentText()
        blocker = QtCore.QSignalBlocker(slot.cycle_spin)
        if mode == "Block mean":
            slot.selector_label.setText("Block idx")
            slot.cycle_spin.setRange(slot.dataset.min_block, slot.dataset.max_block)
            if selected_cycle is None:
                selected_cycle = slot.dataset.min_cycle
            selected_cycle = _nearest_available_cycle(int(selected_cycle), slot.dataset.available_cycles)
            selected_block = ((selected_cycle - 1) // self.cycle_block_size) + 1
            selected_block = min(max(selected_block, slot.dataset.min_block), slot.dataset.max_block)
            slot.cycle_spin.setValue(selected_block)
        else:
            slot.selector_label.setText("Cycle ref")
            slot.cycle_spin.setRange(slot.dataset.min_cycle, slot.dataset.max_cycle)
            if selected_cycle is None:
                current = int(slot.cycle_spin.value()) if slot.cycle_spin.value() else slot.dataset.min_cycle
                selected_cycle = current
            selected_cycle = _nearest_available_cycle(int(selected_cycle), slot.dataset.available_cycles)
            slot.cycle_spin.setValue(selected_cycle)
        del blocker

    def _on_compare_mode_changed(self, slot_index: int) -> None:
        slot = self.compare_slots[slot_index]
        if slot.dataset is None:
            return

        current_value = int(slot.cycle_spin.value())
        if slot.mode_combo.currentText() == "Block mean":
            selected_cycle = _nearest_available_cycle(current_value, slot.dataset.available_cycles)
        else:
            block_index = min(max(current_value, slot.dataset.min_block), slot.dataset.max_block)
            selected_cycle = ((block_index - 1) * self.cycle_block_size) + 1
            selected_cycle = _nearest_available_cycle(selected_cycle, slot.dataset.available_cycles)

        self._configure_compare_selector(slot, selected_cycle=selected_cycle)
        self._update_compare_plots()

    def _clear_compare_slot(self, slot_index: int) -> None:
        slot = self.compare_slots[slot_index]
        slot.dataset = None
        slot.file_label.setText("No file loaded")
        slot.summary_label.setText("Empty")
        slot.mode_combo.setEnabled(False)
        slot.cycle_spin.setEnabled(False)
        slot.clear_button.setEnabled(False)
        slot.selector_label.setText("Cycle ref")
        blocker = QtCore.QSignalBlocker(slot.cycle_spin)
        slot.cycle_spin.setRange(1, 1)
        slot.cycle_spin.setValue(1)
        del blocker
        if slot.pcyl_curve is not None:
            slot.pcyl_curve.hide()
            slot.pcyl_curve.setData([], [])
        if slot.q1_curve is not None:
            slot.q1_curve.hide()
            slot.q1_curve.setData([], [])
        self._update_compare_plots()

    def _copy_current_to_slot_one(self) -> None:
        self._assign_compare_slot_dataset(0, self.dataset, default_cycle=self.current_cycle or self.initial_cycle)
        self._update_compare_plots()

    def _open_compare_csv_dialog(self, slot_index: int) -> None:
        slot = self.compare_slots[slot_index]
        start_dir = self.csv_path.parent
        if slot.dataset is not None:
            start_dir = slot.dataset.csv_path.parent
        selected, _ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            f"Open KIBOX CSV for trace {slot.slot_index}",
            str(start_dir),
            "CSV Files (*.csv);;All Files (*.*)",
        )
        if not selected:
            return

        csv_path = Path(selected)
        self.setCursor(QtCore.Qt.CursorShape.WaitCursor)
        try:
            dataset = self._load_dataset(csv_path)
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "Open CSV", f"Failed to load file:\n{csv_path}\n\n{exc}")
            return
        finally:
            self.unsetCursor()

        self._assign_compare_slot_dataset(slot_index, dataset)
        self._update_compare_plots()

    def _resolve_compare_selection(
        self,
        slot: CompareSlot,
        series: ViewerSeries,
    ) -> CompareSelection | None:
        if slot.dataset is None:
            return None

        mode = slot.mode_combo.currentText()
        stem = slot.dataset.csv_path.stem

        if mode == "Cycle":
            cycle_reference = int(slot.cycle_spin.value())
            selected_cycle = _nearest_available_cycle(cycle_reference, slot.dataset.available_cycles)
            if selected_cycle != cycle_reference:
                blocker = QtCore.QSignalBlocker(slot.cycle_spin)
                slot.cycle_spin.setValue(selected_cycle)
                del blocker
            curve = series.cycle_lookup.get(selected_cycle)
            if curve is None:
                return None
            return CompareSelection(
                x=curve.x,
                y=curve.y,
                label=f"{stem} | Cycle {selected_cycle}",
                summary=f"Cycle {selected_cycle}",
                mode=mode,
                cycle_reference=cycle_reference,
                selected_cycle=selected_cycle,
                block_label=None,
                csv_path=slot.dataset.csv_path,
            )

        block_index = int(slot.cycle_spin.value())
        block_curve = series.block_lookup.get(block_index)
        if block_curve is None:
            return None
        selected_cycle = ((block_index - 1) * self.cycle_block_size) + 1
        selected_cycle = _nearest_available_cycle(selected_cycle, slot.dataset.available_cycles)
        return CompareSelection(
            x=block_curve.x,
            y=block_curve.y,
            label=f"{stem} | Mean {block_curve.label}",
            summary=f"Mean {block_curve.label}",
            mode=mode,
            cycle_reference=block_index,
            selected_cycle=selected_cycle,
            block_label=block_curve.label,
            csv_path=slot.dataset.csv_path,
        )

    @staticmethod
    def _compute_selection_limits(
        selections: list[CompareSelection],
        fallback: tuple[float, float],
    ) -> tuple[float, float]:
        arrays = [selection.y.astype(float, copy=False) for selection in selections if selection.y.size]
        if not arrays:
            return fallback

        all_values = np.concatenate(arrays)
        ymin = float(np.nanmin(all_values))
        ymax = float(np.nanmax(all_values))
        if np.isclose(ymin, ymax):
            pad = max(abs(ymin) * 0.05, 1.0)
            return (ymin - pad, ymax + pad)
        pad = (ymax - ymin) * 0.05
        return (ymin - pad, ymax + pad)

    def _update_compare_plot(
        self,
        *,
        plot: pg.PlotItem,
        legend: pg.LegendItem,
        slots: list[CompareSlot],
        selection_getter,
        fallback_limits: tuple[float, float],
        title: str,
    ) -> None:
        visible_selections: list[CompareSelection] = []
        for slot in slots:
            selection = selection_getter(slot)
            curve = slot.pcyl_curve if plot is self.compare_pcyl_plot else slot.q1_curve
            if curve is None:
                continue
            if selection is None:
                curve.hide()
                curve.setData([], [])
                continue

            pen_style = QtCore.Qt.PenStyle.SolidLine if selection.mode == "Cycle" else QtCore.Qt.PenStyle.DashLine
            curve.setPen(pg.mkPen(color=slot.color, width=1.5, style=pen_style))
            curve.setData(selection.x, selection.y)
            curve.show()
            visible_selections.append(selection)

        legend.clear()
        for slot in slots:
            curve = slot.pcyl_curve if plot is self.compare_pcyl_plot else slot.q1_curve
            selection = selection_getter(slot)
            if curve is not None and selection is not None:
                legend.addItem(curve, selection.label)

        ymin, ymax = self._compute_selection_limits(visible_selections, fallback_limits)
        plot.setYRange(ymin, ymax, padding=0.0)
        plot.setTitle(title if visible_selections else f"{title} - no active traces")

    def _update_compare_plots(self) -> None:
        slot_summaries: dict[int, str] = {}
        pcyl_cache: dict[int, CompareSelection | None] = {}
        q1_cache: dict[int, CompareSelection | None] = {}

        for slot in self.compare_slots:
            pcyl_selection = self._resolve_compare_selection(slot, slot.dataset.pcyl_series) if slot.dataset else None
            q1_selection = self._resolve_compare_selection(slot, slot.dataset.q1_series) if slot.dataset else None
            pcyl_cache[slot.slot_index] = pcyl_selection
            q1_cache[slot.slot_index] = q1_selection
            selection = pcyl_selection or q1_selection
            if selection is None:
                slot_summaries[slot.slot_index] = "Empty" if slot.dataset is None else "No data for selection"
            else:
                slot_summaries[slot.slot_index] = selection.summary

        for slot in self.compare_slots:
            slot.summary_label.setText(slot_summaries[slot.slot_index])

        self._update_compare_plot(
            plot=self.compare_pcyl_plot,
            legend=self.compare_pcyl_legend,
            slots=self.compare_slots,
            selection_getter=lambda slot: pcyl_cache[slot.slot_index],
            fallback_limits=self.pcyl_series.y_limits,
            title="PCYL_1 comparison",
        )
        self._update_compare_plot(
            plot=self.compare_q1_plot,
            legend=self.compare_q1_legend,
            slots=self.compare_slots,
            selection_getter=lambda slot: q1_cache[slot.slot_index],
            fallback_limits=self.q1_series.y_limits,
            title="Q_1 comparison",
        )

    def _export_compare_plots(self) -> None:
        active_slots = [slot for slot in self.compare_slots if slot.dataset is not None]
        if not active_slots:
            QtWidgets.QMessageBox.warning(self, "Export Compare", "Load at least one compare trace before exporting.")
            return

        selected_dir = QtWidgets.QFileDialog.getExistingDirectory(
            self,
            "Choose export directory",
            str(self.csv_path.parent),
        )
        if not selected_dir:
            return

        export_dir = Path(selected_dir)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prefix = f"kibox_compare_{timestamp}"

        pcyl_path = export_dir / f"{prefix}_pcyl.png"
        q1_path = export_dir / f"{prefix}_q1.png"
        selection_path = export_dir / f"{prefix}_selection.csv"

        self._export_plot_item_png(self.compare_pcyl_plot, pcyl_path, COMPARE_EXPORT_SIZE)
        self._export_plot_item_png(self.compare_q1_plot, q1_path, COMPARE_EXPORT_SIZE)

        rows: list[dict[str, object]] = []
        for slot in active_slots:
            selection = self._resolve_compare_selection(slot, slot.dataset.pcyl_series) or self._resolve_compare_selection(
                slot, slot.dataset.q1_series
            )
            if selection is None:
                continue
            rows.append(
                {
                    "slot": slot.slot_index,
                    "csv_path": str(slot.dataset.csv_path),
                    "mode": selection.mode,
                    "selection_index": selection.cycle_reference,
                    "selected_cycle": selection.selected_cycle,
                    "block_label": selection.block_label,
                    "summary": selection.summary,
                }
            )
        pd.DataFrame(rows).to_csv(selection_path, index=False)

        QtWidgets.QMessageBox.information(
            self,
            "Export Compare",
            "Export complete:\n"
            f"- {pcyl_path.name}\n"
            f"- {q1_path.name}\n"
            f"- {selection_path.name}",
        )

    @staticmethod
    def _export_plot_item_png(
        plot_item: pg.PlotItem,
        out_path: Path,
        size: tuple[int, int],
    ) -> None:
        def _to_qcolor(value: object, fallback: QtGui.QColor) -> QtGui.QColor:
            if isinstance(value, QtGui.QColor):
                return value
            if isinstance(value, str):
                c = QtGui.QColor(value)
                return c if c.isValid() else fallback
            if isinstance(value, (tuple, list)):
                if len(value) >= 4:
                    return QtGui.QColor(int(value[0]), int(value[1]), int(value[2]), int(value[3]))
                if len(value) >= 3:
                    return QtGui.QColor(int(value[0]), int(value[1]), int(value[2]))
            return fallback

        QtWidgets.QApplication.processEvents()
        source = plot_item.sceneBoundingRect()
        if source.width() <= 0 or source.height() <= 0:
            raise ValueError("Invalid plot bounds for export.")

        max_width, max_height = size
        src_ratio = float(source.width()) / float(source.height())
        export_width = int(max_width)
        export_height = max(1, int(round(export_width / src_ratio)))
        if export_height > max_height:
            export_height = int(max_height)
            export_width = max(1, int(round(export_height * src_ratio)))

        bg_value = plot_item.getViewBox().state.get("background")
        bg_color = _to_qcolor(bg_value, QtGui.QColor(8, 8, 8))

        image = QtGui.QImage(export_width, export_height, QtGui.QImage.Format.Format_ARGB32)
        image.fill(bg_color)

        painter = QtGui.QPainter(image)
        painter.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing, True)
        target = QtCore.QRectF(0, 0, export_width, export_height)
        plot_item.scene().render(
            painter,
            target,
            source,
            QtCore.Qt.AspectRatioMode.KeepAspectRatio,
        )
        painter.end()

        image.save(str(out_path))

    def _apply_dataset(self, dataset: ViewerDataset, initial_cycle: int | None = None) -> None:
        self.dataset_cache[dataset.csv_path.resolve()] = dataset
        self.dataset = dataset
        self.csv_path = dataset.csv_path
        self.pcyl_series = dataset.pcyl_series
        self.pv_series = dataset.pv_series
        self.q1_series = dataset.q1_series
        self.pmax_cycles = dataset.pmax_cycles
        self.pmax_values = dataset.pmax_values
        self.pmax_map = dataset.pmax_map
        self.available_cycles = dataset.available_cycles
        self.min_cycle = dataset.min_cycle
        self.max_cycle = dataset.max_cycle
        self.cycle_to_index = {int(c): i for i, c in enumerate(self.available_cycles.tolist())}

        target_cycle = self.current_cycle if initial_cycle is None else initial_cycle
        self.initial_cycle = _nearest_available_cycle(int(target_cycle), self.available_cycles)
        self.file_label.setText(self.csv_path.name)
        self.setWindowTitle(f"Fast KIBOX cycle viewer - {self.csv_path.name}")

        self.pcyl_plot.setXRange(self.pcyl_series.x_min, self.pcyl_series.x_max, padding=0.0)
        self.q1_plot.setXRange(self.q1_series.x_min, self.q1_series.x_max, padding=0.0)
        self.pcyl_plot.setYRange(*self.pcyl_series.y_limits, padding=0.0)
        self.q1_plot.setYRange(*self.q1_series.y_limits, padding=0.0)
        self.pv_plot.setXRange(*_log10_range(self.pv_series.x_limits), padding=0.0)
        self.pv_plot.setYRange(*_log10_range(self.pv_series.y_limits), padding=0.0)

        pmax_min = float(np.nanmin(self.pmax_values))
        pmax_max = float(np.nanmax(self.pmax_values))
        pmax_pad = max((pmax_max - pmax_min) * 0.05, 1.0)
        self.pmax_plot.setXRange(float(self.min_cycle), float(self.max_cycle), padding=0.0)
        self.pmax_plot.setYRange(pmax_min - pmax_pad, pmax_max + pmax_pad, padding=0.0)
        self.pmax_curve.setData(self.pmax_cycles, self.pmax_values)
        self.pmax_cursor.setValue(float(self.initial_cycle))

        self._syncing = True
        self.slider.setMinimum(self.min_cycle)
        self.slider.setMaximum(self.max_cycle)
        self.slider.setValue(self.initial_cycle)
        self.cycle_edit.setText(str(self.initial_cycle))
        self._syncing = False
        self.current_cycle = None
        self.update_cycle(self.initial_cycle)

    def _open_csv_dialog(self) -> None:
        selected, _ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            "Open KIBOX CSV",
            str(self.csv_path.parent),
            "CSV Files (*.csv);;All Files (*.*)",
        )
        if not selected:
            return

        csv_path = Path(selected)
        self.setCursor(QtCore.Qt.CursorShape.WaitCursor)
        try:
            dataset = self._load_dataset(csv_path)
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "Open CSV", f"Failed to load file:\n{csv_path}\n\n{exc}")
            return
        finally:
            self.unsetCursor()

        self._apply_dataset(dataset)

    def _on_slider_changed(self, value: int) -> None:
        if self._syncing:
            return
        self.update_cycle(int(value))

    def _on_cycle_edit_submitted(self) -> None:
        text = self.cycle_edit.text().strip()
        if not text:
            self.cycle_edit.setText(str(self.current_cycle))
            return
        try:
            requested = int(round(float(text)))
        except ValueError:
            self.cycle_edit.setText(str(self.current_cycle))
            return
        self.update_cycle(requested)

    def keyPressEvent(self, event) -> None:  # type: ignore[override]
        idx = self.cycle_to_index.get(self.current_cycle)
        if event.key() == QtCore.Qt.Key.Key_Left and idx is not None and idx > 0:
            self.update_cycle(int(self.available_cycles[idx - 1]))
            return
        if event.key() == QtCore.Qt.Key.Key_Right and idx is not None and idx < len(self.available_cycles) - 1:
            self.update_cycle(int(self.available_cycles[idx + 1]))
            return
        super().keyPressEvent(event)

    def update_cycle(self, requested_cycle: int) -> None:
        cycle = _nearest_available_cycle(requested_cycle, self.available_cycles)
        if self.current_cycle is not None and cycle == self.current_cycle and not self._syncing:
            return

        block_index = ((cycle - 1) // self.cycle_block_size) + 1
        pcyl_cycle = self.pcyl_series.cycle_lookup.get(cycle)
        pv_cycle = self.pv_series.cycle_lookup.get(cycle)
        q1_cycle = self.q1_series.cycle_lookup.get(cycle)
        pcyl_block = self.pcyl_series.block_lookup.get(block_index)
        pv_block = self.pv_series.block_lookup.get(block_index)
        q1_block = self.q1_series.block_lookup.get(block_index)

        if pcyl_cycle is not None:
            self.pcyl_curve.setData(pcyl_cycle.x, pcyl_cycle.y)
        else:
            self.pcyl_curve.setData([], [])

        if pv_cycle is not None:
            self.pv_curve.setData(pv_cycle.x, pv_cycle.y)
        else:
            self.pv_curve.setData([], [])

        if q1_cycle is not None:
            self.q1_curve.setData(q1_cycle.x, q1_cycle.y)
        else:
            self.q1_curve.setData([], [])

        if self.show_block_mean and pcyl_block is not None:
            self.pcyl_block_curve.setData(pcyl_block.x, pcyl_block.y)
            self.pcyl_block_curve.show()
        else:
            self.pcyl_block_curve.hide()

        if self.show_block_mean and pv_block is not None:
            self.pv_block_curve.setData(pv_block.x, pv_block.y)
            self.pv_block_curve.show()
        else:
            self.pv_block_curve.hide()

        if self.show_block_mean and q1_block is not None:
            self.q1_block_curve.setData(q1_block.x, q1_block.y)
            self.q1_block_curve.show()
        else:
            self.q1_block_curve.hide()

        block_label = "n/a"
        if pcyl_block is not None:
            block_label = pcyl_block.label
        elif q1_block is not None:
            block_label = q1_block.label

        self.pmax_cursor.setValue(float(cycle))
        pmax_value = self.pmax_map.get(cycle)
        if pmax_value is not None:
            self.pmax_point.setData([{"pos": (float(cycle), float(pmax_value))}])
        else:
            self.pmax_point.clear()

        self.pcyl_plot.setTitle(f"PCYL_1 - Cycle {cycle} (block {block_label})")
        self.pv_plot.setTitle(f"Log10 P-V - Cycle {cycle} (block {block_label})")
        self.q1_plot.setTitle(f"Q_1 - Cycle {cycle} (block {block_label})")
        self.pmax_plot.setTitle(f"PMAX by cycle - selected cycle {cycle}")

        self._syncing = True
        self.slider.setValue(int(cycle))
        self.cycle_edit.setText(str(cycle))
        self._syncing = False
        self.current_cycle = cycle


def main() -> None:
    args = parse_args()
    csv_path = args.input.expanduser().resolve()
    if not csv_path.exists() or not csv_path.is_file():
        if args.no_show:
            raise SystemExit(f"Input file not found: {csv_path}")

        print(f"[WARN] Input padrao indisponivel: {csv_path}")
        selected_path = _prompt_input_csv(csv_path)
        if selected_path is None:
            raise SystemExit("Nenhum CSV selecionado. Execucao cancelada.")
        csv_path = selected_path

    app: QtWidgets.QApplication | None = None
    loading_dialog: QtWidgets.QProgressDialog | None = None
    if not args.no_show:
        app = QtWidgets.QApplication.instance() or QtWidgets.QApplication([])
        app.setApplicationName("Fast KIBOX Cycle Viewer")
        loading_dialog = QtWidgets.QProgressDialog(
            f"Loading KIBOX CSV...\n{csv_path.name}",
            None,
            0,
            0,
        )
        loading_dialog.setWindowTitle("Fast KIBOX Cycle Viewer")
        loading_dialog.setCancelButton(None)
        loading_dialog.setMinimumDuration(0)
        loading_dialog.setWindowModality(QtCore.Qt.WindowModality.ApplicationModal)
        loading_dialog.setValue(0)
        loading_dialog.show()
        QtWidgets.QApplication.processEvents()

    print(f"[INFO] Loading viewer dataset: {csv_path}")
    started_at = time.perf_counter()
    try:
        dataset = prepare_viewer_dataset(csv_path, args.cycle_block_size)
    except Exception as exc:
        if loading_dialog is not None:
            loading_dialog.close()
        message = f"Falha ao abrir CSV do viewer:\n{csv_path}\n\n{exc}"
        print(f"[ERROR] {message}", file=sys.stderr)
        if not args.no_show:
            app = app or QtWidgets.QApplication.instance() or QtWidgets.QApplication([])
            QtWidgets.QMessageBox.critical(None, "Fast KIBOX Cycle Viewer", message)
        raise SystemExit(1) from exc
    finally:
        if loading_dialog is not None:
            loading_dialog.close()

    elapsed_s = time.perf_counter() - started_at
    print(f"[INFO] Viewer dataset ready in {elapsed_s:.1f}s: {csv_path.name}")

    if args.no_show:
        print(f"[OK] Fast viewer prepared for {csv_path}")
        print(f"[OK] Available cycles: {len(dataset.available_cycles)}")
        print(f"[OK] Initial cycle: {args.initial_cycle}")
        print(f"[OK] Block mean overlay: {not args.hide_block_mean}")
        return

    app = app or QtWidgets.QApplication.instance() or QtWidgets.QApplication([])
    app.setApplicationName("Fast KIBOX Cycle Viewer")
    viewer = FastCycleViewer(
        dataset=dataset,
        cycle_block_size=args.cycle_block_size,
        initial_cycle=args.initial_cycle,
        show_block_mean=not args.hide_block_mean,
    )
    viewer.show()
    app.exec()


if __name__ == "__main__":
    main()
