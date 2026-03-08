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
from matplotlib.widgets import Slider
from standalone_kibox_cycle_plots import (
    DEFAULT_CYCLE_BLOCK_SIZE,
    DEFAULT_INPUT,
    load_cycle_dataframe,
    mean_curve_by_cycle_block,
)


@dataclass
class ViewerSeries:
    cycle_lookup: dict[int, pd.DataFrame]
    block_lookup: dict[int, pd.DataFrame]
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


def _build_cycle_lookup(df: pd.DataFrame, value_col: str, x_min: float, x_max: float) -> dict[int, pd.DataFrame]:
    per_cycle = (
        df.dropna(subset=[value_col])
        .groupby(["CycleNumber", "CrankAngle_deg"], as_index=False)[value_col]
        .mean()
    )
    per_cycle = per_cycle[
        (per_cycle["CrankAngle_deg"] >= x_min) & (per_cycle["CrankAngle_deg"] <= x_max)
    ].sort_values(["CycleNumber", "CrankAngle_deg"])
    return {
        int(cycle): group[["CrankAngle_deg", value_col]].reset_index(drop=True)
        for cycle, group in per_cycle.groupby("CycleNumber", sort=True)
    }


def _build_block_lookup(df: pd.DataFrame, value_col: str, cycle_block_size: int, x_min: float, x_max: float) -> dict[int, pd.DataFrame]:
    block_curve = mean_curve_by_cycle_block(df, value_col, cycle_block_size)
    block_curve = block_curve[
        (block_curve["CrankAngle_deg"] >= x_min) & (block_curve["CrankAngle_deg"] <= x_max)
    ].sort_values(["CycleBlockIndex", "CrankAngle_deg"])
    return {
        int(block_index): group[["CrankAngle_deg", "mean_value", "CycleBlockLabel"]].reset_index(drop=True)
        for block_index, group in block_curve.groupby("CycleBlockIndex", sort=True)
    }


def _compute_y_limits(
    cycle_lookup: dict[int, pd.DataFrame],
    block_lookup: dict[int, pd.DataFrame],
    *,
    value_col: str,
) -> tuple[float, float]:
    values: list[np.ndarray] = []
    for d in cycle_lookup.values():
        arr = pd.to_numeric(d[value_col], errors="coerce").dropna().to_numpy(dtype=float)
        if arr.size:
            values.append(arr)
    for d in block_lookup.values():
        arr = pd.to_numeric(d["mean_value"], errors="coerce").dropna().to_numpy(dtype=float)
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
    df: pd.DataFrame,
    *,
    value_col: str,
    y_label: str,
    x_range: tuple[float, float],
    cycle_block_size: int,
) -> ViewerSeries:
    x_min, x_max = x_range
    cycle_lookup = _build_cycle_lookup(df, value_col, x_min, x_max)
    block_lookup = _build_block_lookup(df, value_col, cycle_block_size, x_min, x_max)
    y_limits = _compute_y_limits(cycle_lookup, block_lookup, value_col=value_col)
    return ViewerSeries(
        cycle_lookup=cycle_lookup,
        block_lookup=block_lookup,
        y_label=y_label,
        x_min=x_min,
        x_max=x_max,
        y_limits=y_limits,
    )


def _nearest_available_cycle(cycle: int, available_cycles: list[int]) -> int:
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
    cycle_block_size: int,
    initial_cycle: int,
    show_block_mean: bool,
    no_show: bool,
) -> None:
    available_cycles = sorted(set(pcyl_series.cycle_lookup) | set(q1_series.cycle_lookup))
    if not available_cycles:
        raise ValueError("No cycles available for viewer.")

    initial_cycle = _nearest_available_cycle(initial_cycle, available_cycles)

    fig, (ax_pcyl, ax_q1) = plt.subplots(2, 1, figsize=(12, 8))
    plt.subplots_adjust(bottom=0.16, top=0.92, left=0.08, right=0.95, hspace=0.32)

    pcyl_cycle_line, = ax_pcyl.plot([], [], color="tab:blue", linewidth=1.1, label="Selected cycle")
    pcyl_block_line, = ax_pcyl.plot([], [], color="black", linestyle="--", linewidth=1.4, label="Block mean")
    q1_cycle_line, = ax_q1.plot([], [], color="tab:orange", linewidth=1.1, label="Selected cycle")
    q1_block_line, = ax_q1.plot([], [], color="black", linestyle="--", linewidth=1.4, label="Block mean")

    for ax, series in ((ax_pcyl, pcyl_series), (ax_q1, q1_series)):
        ax.set_xlim(series.x_min, series.x_max)
        ax.set_ylim(*series.y_limits)
        ax.set_xlabel("Crank angle (deg CA)")
        ax.set_ylabel(series.y_label)
        ax.grid(True, which="both", linestyle="--", linewidth=0.5)
        ax.legend(loc="upper right")

    fig.suptitle(csv_path.name)

    slider_ax = fig.add_axes([0.12, 0.06, 0.76, 0.03])
    cycle_slider = Slider(
        ax=slider_ax,
        label="Cycle",
        valmin=float(min(available_cycles)),
        valmax=float(max(available_cycles)),
        valinit=float(initial_cycle),
        valstep=1.0,
    )

    def update(cycle_value: float) -> None:
        cycle = _nearest_available_cycle(int(round(cycle_value)), available_cycles)
        block_index = ((cycle - 1) // cycle_block_size) + 1

        pcyl_cycle = pcyl_series.cycle_lookup.get(cycle, pd.DataFrame(columns=["CrankAngle_deg", "PCYL_1"]))
        q1_cycle = q1_series.cycle_lookup.get(cycle, pd.DataFrame(columns=["CrankAngle_deg", "Q_1"]))
        pcyl_block = pcyl_series.block_lookup.get(block_index, pd.DataFrame(columns=["CrankAngle_deg", "mean_value", "CycleBlockLabel"]))
        q1_block = q1_series.block_lookup.get(block_index, pd.DataFrame(columns=["CrankAngle_deg", "mean_value", "CycleBlockLabel"]))

        pcyl_cycle_line.set_data(pcyl_cycle.get("CrankAngle_deg", []), pcyl_cycle.get("PCYL_1", []))
        q1_cycle_line.set_data(q1_cycle.get("CrankAngle_deg", []), q1_cycle.get("Q_1", []))

        if show_block_mean and not pcyl_block.empty:
            pcyl_block_line.set_data(pcyl_block["CrankAngle_deg"], pcyl_block["mean_value"])
            pcyl_block_line.set_visible(True)
        else:
            pcyl_block_line.set_data([], [])
            pcyl_block_line.set_visible(False)

        if show_block_mean and not q1_block.empty:
            q1_block_line.set_data(q1_block["CrankAngle_deg"], q1_block["mean_value"])
            q1_block_line.set_visible(True)
        else:
            q1_block_line.set_data([], [])
            q1_block_line.set_visible(False)

        block_label = "n/a"
        if not pcyl_block.empty:
            block_label = str(pcyl_block["CycleBlockLabel"].iloc[0])
        elif not q1_block.empty:
            block_label = str(q1_block["CycleBlockLabel"].iloc[0])

        ax_pcyl.set_title(f"PCYL_1 - Cycle {cycle} (block {block_label})")
        ax_q1.set_title(f"Q_1 - Cycle {cycle} (block {block_label})")
        fig.canvas.draw_idle()

    def on_key(event) -> None:
        if event.key not in {"left", "right"}:
            return
        current = _nearest_available_cycle(int(round(cycle_slider.val)), available_cycles)
        idx = available_cycles.index(current)
        if event.key == "left" and idx > 0:
            cycle_slider.set_val(float(available_cycles[idx - 1]))
        elif event.key == "right" and idx < len(available_cycles) - 1:
            cycle_slider.set_val(float(available_cycles[idx + 1]))

    cycle_slider.on_changed(update)
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
    pcyl_series = build_viewer_series(
        df,
        value_col="PCYL_1",
        y_label="P_CYL (bar)",
        x_range=PCYL_X_RANGE,
        cycle_block_size=args.cycle_block_size,
    )
    q1_series = build_viewer_series(
        df,
        value_col="Q_1",
        y_label="Q_1 (J/deg CA)",
        x_range=Q1_X_RANGE,
        cycle_block_size=args.cycle_block_size,
    )

    launch_viewer(
        csv_path,
        pcyl_series=pcyl_series,
        q1_series=q1_series,
        cycle_block_size=args.cycle_block_size,
        initial_cycle=args.initial_cycle,
        show_block_mean=not args.hide_block_mean,
        no_show=args.no_show,
    )


if __name__ == "__main__":
    main()
