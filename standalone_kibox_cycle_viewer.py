from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import sys

import matplotlib
import numpy as np
import pandas as pd


PCYL_X_RANGE = (-40.0, 80.0)
Q1_X_RANGE = (-30.0, 90.0)


def _can_use_tk_backend() -> bool:
    try:
        import tkinter as tk

        root = tk.Tk()
        root.withdraw()
        root.update_idletasks()
        root.destroy()
        return True
    except Exception:
        return False


def _select_backend() -> str:
    if "--no-show" in sys.argv:
        matplotlib.use("Agg")
        return "Agg"

    if _can_use_tk_backend():
        matplotlib.use("TkAgg")
        return "TkAgg"

    matplotlib.use("WebAgg")
    return "WebAgg"


SELECTED_BACKEND = _select_backend()

import matplotlib.pyplot as plt
from matplotlib.widgets import Slider, TextBox
from standalone_kibox_cycle_plots import (
    DEFAULT_CYCLE_BLOCK_SIZE,
    DEFAULT_INPUT,
    load_cycle_dataframe,
)


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
    value_col: str
    y_label: str
    x_min: float
    x_max: float
    y_limits: tuple[float, float]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Interactive cycle-by-cycle KIBOX viewer with fixed plot scales."
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
        help="Prepare the viewer and exit without opening the GUI window.",
    )
    return parser.parse_args()


def build_per_cycle_means(df: pd.DataFrame) -> pd.DataFrame:
    per_cycle = (
        df.groupby(["CycleNumber", "CrankAngle_deg"], as_index=False, sort=False)
        .agg(
            PCYL_1=("PCYL_1", "mean"),
            Q_1=("Q_1", "mean"),
        )
        .sort_values(["CycleNumber", "CrankAngle_deg"])
    )
    per_cycle["CycleNumber"] = per_cycle["CycleNumber"].astype(np.int32)
    per_cycle["CrankAngle_deg"] = per_cycle["CrankAngle_deg"].astype(np.float32)
    per_cycle["PCYL_1"] = pd.to_numeric(per_cycle["PCYL_1"], errors="coerce", downcast="float")
    per_cycle["Q_1"] = pd.to_numeric(per_cycle["Q_1"], errors="coerce", downcast="float")
    return per_cycle


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


def _compute_y_limits(
    cycle_lookup: dict[int, CurveData],
    block_lookup: dict[int, BlockCurveData],
) -> tuple[float, float]:
    values: list[np.ndarray] = []
    for d in cycle_lookup.values():
        arr = np.asarray(d.y, dtype=float)
        if arr.size:
            values.append(arr)
    for d in block_lookup.values():
        arr = np.asarray(d.y, dtype=float)
        if arr.size:
            values.append(arr)

    if not values:
        return (0.0, 1.0)

    all_values = np.concatenate(values)
    ymin = float(np.nanmin(all_values))
    ymax = float(np.nanmax(all_values))
    if not np.isfinite(ymin) or not np.isfinite(ymax):
        return (0.0, 1.0)
    if np.isclose(ymin, ymax):
        pad = max(abs(ymin) * 0.05, 1.0)
        return (ymin - pad, ymax + pad)
    pad = (ymax - ymin) * 0.05
    return (ymin - pad, ymax + pad)


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
        value_col=value_col,
        y_label=y_label,
        x_min=x_min,
        x_max=x_max,
        y_limits=y_limits,
    )


def build_pmax_series(df: pd.DataFrame) -> pd.DataFrame:
    pmax = (
        df.dropna(subset=["PCYL_1"])
        .groupby("CycleNumber", as_index=False)["PCYL_1"]
        .max()
        .rename(columns={"PCYL_1": "PMAX_bar"})
        .sort_values("CycleNumber")
    )
    pmax["CycleNumber"] = pmax["CycleNumber"].astype(int)
    return pmax


def _nearest_available_cycle(cycle: int, available_cycles: list[int] | np.ndarray) -> int:
    if cycle in available_cycles:
        return cycle
    arr = np.asarray(available_cycles, dtype=int)
    idx = int(np.argmin(np.abs(arr - cycle)))
    return int(arr[idx])


def launch_viewer(
    csv_path: Path,
    *,
    pcyl_series: ViewerSeries,
    q1_series: ViewerSeries,
    pmax_series: pd.DataFrame,
    cycle_block_size: int,
    initial_cycle: int,
    show_block_mean: bool,
    no_show: bool,
) -> None:
    available_cycles = sorted(set(pcyl_series.cycle_lookup) | set(q1_series.cycle_lookup))
    if not available_cycles:
        raise ValueError("No cycles available for viewer.")

    available_cycles_arr = np.asarray(available_cycles, dtype=np.int32)
    cycle_to_index = {cycle: idx for idx, cycle in enumerate(available_cycles)}
    pmax_map = dict(zip(pmax_series["CycleNumber"].astype(int), pmax_series["PMAX_bar"].astype(float)))
    initial_cycle = _nearest_available_cycle(initial_cycle, available_cycles)

    fig, (ax_pcyl, ax_q1, ax_pmax) = plt.subplots(3, 1, figsize=(12, 10))
    plt.subplots_adjust(bottom=0.16, top=0.94, left=0.08, right=0.95, hspace=0.36)

    pcyl_cycle_line, = ax_pcyl.plot([], [], color="tab:blue", linewidth=1.1, label="Selected cycle")
    pcyl_block_line, = ax_pcyl.plot([], [], color="black", linestyle="--", linewidth=1.4, label="Block mean")
    q1_cycle_line, = ax_q1.plot([], [], color="tab:orange", linewidth=1.1, label="Selected cycle")
    q1_block_line, = ax_q1.plot([], [], color="black", linestyle="--", linewidth=1.4, label="Block mean")
    ax_pmax.plot(
        pmax_series["CycleNumber"].to_numpy(),
        pmax_series["PMAX_bar"].to_numpy(),
        color="tab:green",
        linewidth=1.0,
        label="PMAX per cycle",
    )
    pmax_cursor = ax_pmax.axvline(float(initial_cycle), color="crimson", linestyle="--", linewidth=1.4, label="Selected cycle")
    pmax_selected_point, = ax_pmax.plot([], [], marker="o", color="crimson", markersize=6, linestyle="None")

    for ax, series in ((ax_pcyl, pcyl_series), (ax_q1, q1_series)):
        ax.set_xlim(series.x_min, series.x_max)
        ax.set_ylim(*series.y_limits)
        ax.set_xlabel("Crank angle (deg CA)")
        ax.set_ylabel(series.y_label)
        ax.grid(True, which="both", linestyle="--", linewidth=0.5)
        ax.legend(loc="upper right")

    pmax_y = pd.to_numeric(pmax_series["PMAX_bar"], errors="coerce").dropna().to_numpy(dtype=float)
    if pmax_y.size:
        pmax_min = float(np.nanmin(pmax_y))
        pmax_max = float(np.nanmax(pmax_y))
        if np.isclose(pmax_min, pmax_max):
            pmax_pad = max(abs(pmax_min) * 0.05, 1.0)
        else:
            pmax_pad = (pmax_max - pmax_min) * 0.05
        ax_pmax.set_ylim(pmax_min - pmax_pad, pmax_max + pmax_pad)
    ax_pmax.set_xlim(float(min(available_cycles)), float(max(available_cycles)))
    ax_pmax.set_xlabel("Cycle number")
    ax_pmax.set_ylabel("PMAX (bar)")
    ax_pmax.grid(True, which="both", linestyle="--", linewidth=0.5)
    ax_pmax.legend(loc="upper right")

    fig.suptitle(csv_path.name)

    slider_ax = fig.add_axes([0.12, 0.06, 0.60, 0.03])
    cycle_slider = Slider(
        ax=slider_ax,
        label="Cycle",
        valmin=float(min(available_cycles)),
        valmax=float(max(available_cycles)),
        valinit=float(initial_cycle),
        valstep=1.0,
    )
    cycle_slider.valtext.set_visible(False)
    cycle_input_ax = fig.add_axes([0.77, 0.052, 0.12, 0.045])
    cycle_input = TextBox(cycle_input_ax, "Go to", initial=str(initial_cycle))
    sync_state = {"busy": False, "last_cycle": None}

    def update(cycle_value: float) -> None:
        if sync_state["busy"]:
            return
        cycle = _nearest_available_cycle(int(round(cycle_value)), available_cycles)
        if sync_state["last_cycle"] == cycle:
            return
        sync_state["busy"] = True
        try:
            block_index = ((cycle - 1) // cycle_block_size) + 1

            pcyl_cycle = pcyl_series.cycle_lookup.get(cycle)
            q1_cycle = q1_series.cycle_lookup.get(cycle)
            pcyl_block = pcyl_series.block_lookup.get(block_index)
            q1_block = q1_series.block_lookup.get(block_index)

            if pcyl_cycle is not None:
                pcyl_cycle_line.set_data(pcyl_cycle.x, pcyl_cycle.y)
            else:
                pcyl_cycle_line.set_data([], [])

            if q1_cycle is not None:
                q1_cycle_line.set_data(q1_cycle.x, q1_cycle.y)
            else:
                q1_cycle_line.set_data([], [])

            if show_block_mean and pcyl_block is not None:
                pcyl_block_line.set_data(pcyl_block.x, pcyl_block.y)
                pcyl_block_line.set_visible(True)
            else:
                pcyl_block_line.set_data([], [])
                pcyl_block_line.set_visible(False)

            if show_block_mean and q1_block is not None:
                q1_block_line.set_data(q1_block.x, q1_block.y)
                q1_block_line.set_visible(True)
            else:
                q1_block_line.set_data([], [])
                q1_block_line.set_visible(False)

            block_label = "n/a"
            if pcyl_block is not None:
                block_label = pcyl_block.label
            elif q1_block is not None:
                block_label = q1_block.label

            pmax_cursor.set_xdata([float(cycle), float(cycle)])
            pmax_value = pmax_map.get(cycle)
            if pmax_value is not None:
                pmax_selected_point.set_data([float(cycle)], [float(pmax_value)])
            else:
                pmax_selected_point.set_data([], [])

            cycle_input.eventson = False
            cycle_input.set_val(str(cycle))
            cycle_input.eventson = True
            ax_pcyl.set_title(f"PCYL_1 - Cycle {cycle} (block {block_label})")
            ax_q1.set_title(f"Q_1 - Cycle {cycle} (block {block_label})")
            ax_pmax.set_title(f"PMAX by cycle - selected cycle {cycle}")
            sync_state["last_cycle"] = cycle
            fig.canvas.draw_idle()
        finally:
            cycle_input.eventson = True
            sync_state["busy"] = False

    def on_key(event) -> None:
        if event.key not in {"left", "right"}:
            return
        current = _nearest_available_cycle(int(round(cycle_slider.val)), available_cycles)
        idx = cycle_to_index[current]
        if event.key == "left" and idx > 0:
            cycle_slider.set_val(float(available_cycles_arr[idx - 1]))
        elif event.key == "right" and idx < len(available_cycles_arr) - 1:
            cycle_slider.set_val(float(available_cycles_arr[idx + 1]))

    def on_submit(text: str) -> None:
        if sync_state["busy"]:
            return
        stripped = text.strip()
        if not stripped:
            cycle_input.set_val(str(_nearest_available_cycle(int(round(cycle_slider.val)), available_cycles)))
            return
        try:
            requested = int(round(float(stripped)))
        except ValueError:
            cycle_input.set_val(str(_nearest_available_cycle(int(round(cycle_slider.val)), available_cycles)))
            return
        cycle_slider.set_val(float(_nearest_available_cycle(requested, available_cycles)))

    cycle_slider.on_changed(update)
    cycle_input.on_submit(on_submit)
    fig.canvas.mpl_connect("key_press_event", on_key)
    update(float(initial_cycle))

    if no_show:
        print(f"[OK] Viewer prepared for {csv_path}")
        print(f"[OK] Backend: {SELECTED_BACKEND}")
        print(f"[OK] Available cycles: {len(available_cycles)}")
        print(f"[OK] Initial cycle: {initial_cycle}")
        print(f"[OK] Block mean overlay: {show_block_mean}")
        plt.close(fig)
        return

    print(f"[INFO] Viewer backend: {SELECTED_BACKEND}")
    if SELECTED_BACKEND == "WebAgg":
        print("[INFO] Tk nao esta funcional neste Python; o viewer sera aberto via navegador local (WebAgg).")
    plt.show()


def main() -> None:
    args = parse_args()
    csv_path = args.input.resolve()
    if not csv_path.exists():
        raise SystemExit(f"Input file not found: {csv_path}")

    df = load_cycle_dataframe(csv_path)
    df["CycleNumber"] = df["CycleNumber"].astype(np.int32)
    df["CrankAngle_deg"] = df["CrankAngle_deg"].astype(np.float32)
    df["PCYL_1"] = pd.to_numeric(df["PCYL_1"], errors="coerce", downcast="float")
    df["Q_1"] = pd.to_numeric(df["Q_1"], errors="coerce", downcast="float")

    per_cycle = build_per_cycle_means(df)
    pmax_series = build_pmax_series(df)
    pcyl_series = build_viewer_series(
        per_cycle,
        value_col="PCYL_1",
        y_label="P_CYL (bar)",
        x_range=PCYL_X_RANGE,
        cycle_block_size=args.cycle_block_size,
    )
    q1_series = build_viewer_series(
        per_cycle,
        value_col="Q_1",
        y_label="Q_1 (J/deg CA)",
        x_range=Q1_X_RANGE,
        cycle_block_size=args.cycle_block_size,
    )

    launch_viewer(
        csv_path,
        pcyl_series=pcyl_series,
        q1_series=q1_series,
        pmax_series=pmax_series,
        cycle_block_size=args.cycle_block_size,
        initial_cycle=args.initial_cycle,
        show_block_mean=not args.hide_block_mean,
        no_show=args.no_show,
    )


if __name__ == "__main__":
    main()
