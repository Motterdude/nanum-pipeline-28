from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import pyqtgraph as pg
from PySide6 import QtCore, QtWidgets

from standalone_kibox_cycle_plots import (
    DEFAULT_CYCLE_BLOCK_SIZE,
    DEFAULT_INPUT,
    load_cycle_dataframe,
)


PCYL_X_RANGE = (-40.0, 80.0)
Q1_X_RANGE = (-30.0, 90.0)


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
class ViewerDataset:
    csv_path: Path
    pcyl_series: ViewerSeries
    q1_series: ViewerSeries
    pmax_cycles: np.ndarray
    pmax_values: np.ndarray
    pmax_map: dict[int, float]
    available_cycles: np.ndarray
    min_cycle: int
    max_cycle: int


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
    return ViewerDataset(
        csv_path=csv_path,
        pcyl_series=pcyl_series,
        q1_series=q1_series,
        pmax_cycles=pmax_cycles,
        pmax_values=pmax_values,
        pmax_map=pmax_map,
        available_cycles=available_cycles,
        min_cycle=int(available_cycles[0]),
        max_cycle=int(available_cycles[-1]),
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
        self.dataset = dataset
        self.csv_path = dataset.csv_path
        self.pcyl_series = dataset.pcyl_series
        self.q1_series = dataset.q1_series
        self.pmax_cycles = dataset.pmax_cycles
        self.pmax_values = dataset.pmax_values
        self.pmax_map = dataset.pmax_map
        self.available_cycles = dataset.available_cycles
        self.min_cycle = dataset.min_cycle
        self.max_cycle = dataset.max_cycle
        self.cycle_to_index = {int(c): i for i, c in enumerate(self.available_cycles.tolist())}
        self.initial_cycle = _nearest_available_cycle(initial_cycle, self.available_cycles)

        self._setup_ui()
        self._configure_plots()
        self.update_cycle(self.initial_cycle)

    def _setup_ui(self) -> None:
        self.setWindowTitle(f"Fast KIBOX cycle viewer - {self.csv_path.name}")
        self.resize(1650, 1050)

        layout = QtWidgets.QVBoxLayout(self)
        self.graphics = pg.GraphicsLayoutWidget()
        layout.addWidget(self.graphics, stretch=1)

        self.pcyl_plot = self.graphics.addPlot(row=0, col=0)
        self.q1_plot = self.graphics.addPlot(row=1, col=0)
        self.pmax_plot = self.graphics.addPlot(row=2, col=0)

        control_layout = QtWidgets.QHBoxLayout()
        layout.addLayout(control_layout)

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
        self.cycle_edit = QtWidgets.QLineEdit(str(self.current_cycle))
        self.cycle_edit.setMaximumWidth(100)
        control_layout.addWidget(self.cycle_edit)

        hint = QtWidgets.QLabel("Left/Right: step one cycle")
        hint.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignVCenter)
        control_layout.addWidget(hint)

        self.slider.valueChanged.connect(self._on_slider_changed)
        self.cycle_edit.returnPressed.connect(self._on_cycle_edit_submitted)

    def _configure_plots(self) -> None:
        pg.setConfigOptions(antialias=True, useOpenGL=True)

        self.pcyl_plot.showGrid(x=True, y=True, alpha=0.25)
        self.q1_plot.showGrid(x=True, y=True, alpha=0.25)
        self.pmax_plot.showGrid(x=True, y=True, alpha=0.25)

        self.pcyl_plot.setLabel("bottom", "Crank angle", units="deg CA")
        self.pcyl_plot.setLabel("left", self.pcyl_series.y_label)
        self.q1_plot.setLabel("bottom", "Crank angle", units="deg CA")
        self.q1_plot.setLabel("left", self.q1_series.y_label)
        self.pmax_plot.setLabel("bottom", "Cycle")
        self.pmax_plot.setLabel("left", "PMAX (bar)")

        self.pcyl_plot.setXRange(self.pcyl_series.x_min, self.pcyl_series.x_max, padding=0.0)
        self.q1_plot.setXRange(self.q1_series.x_min, self.q1_series.x_max, padding=0.0)
        self.pcyl_plot.setYRange(*self.pcyl_series.y_limits, padding=0.0)
        self.q1_plot.setYRange(*self.q1_series.y_limits, padding=0.0)

        pmax_min = float(np.nanmin(self.pmax_values))
        pmax_max = float(np.nanmax(self.pmax_values))
        pmax_pad = max((pmax_max - pmax_min) * 0.05, 1.0)
        self.pmax_plot.setXRange(float(self.min_cycle), float(self.max_cycle), padding=0.0)
        self.pmax_plot.setYRange(pmax_min - pmax_pad, pmax_max + pmax_pad, padding=0.0)

        blue = pg.mkPen(color=(31, 119, 180), width=1)
        orange = pg.mkPen(color=(255, 127, 14), width=1)
        black = pg.mkPen(color=(40, 40, 40), width=0.9, style=QtCore.Qt.PenStyle.DashLine)
        green = pg.mkPen(color=(44, 160, 44), width=1)
        crimson = pg.mkPen(color=(220, 20, 60), width=1)

        self.pcyl_curve = self.pcyl_plot.plot(pen=blue, name="Selected cycle")
        self.pcyl_block_curve = self.pcyl_plot.plot(pen=black, name="Block mean")
        self.q1_curve = self.q1_plot.plot(pen=orange, name="Selected cycle")
        self.q1_block_curve = self.q1_plot.plot(pen=black, name="Block mean")
        self.pmax_curve = self.pmax_plot.plot(self.pmax_cycles, self.pmax_values, pen=green, name="PMAX per cycle")
        self.pmax_cursor = pg.InfiniteLine(pos=float(self.initial_cycle), angle=90, pen=crimson)
        self.pmax_plot.addItem(self.pmax_cursor)
        self.pmax_point = pg.ScatterPlotItem(size=6, brush=pg.mkBrush(220, 20, 60), pen=pg.mkPen(None))
        self.pmax_plot.addItem(self.pmax_point)

        if not self.show_block_mean:
            self.pcyl_block_curve.hide()
            self.q1_block_curve.hide()

        legend1 = self.pcyl_plot.addLegend(offset=(10, 10))
        legend1.addItem(self.pcyl_curve, "Selected cycle")
        legend1.addItem(self.pcyl_block_curve, "Block mean")

        legend2 = self.q1_plot.addLegend(offset=(10, 10))
        legend2.addItem(self.q1_curve, "Selected cycle")
        legend2.addItem(self.q1_block_curve, "Block mean")

        legend3 = self.pmax_plot.addLegend(offset=(10, 10))
        legend3.addItem(self.pmax_curve, "PMAX per cycle")

    def _apply_dataset(self, dataset: ViewerDataset, initial_cycle: int | None = None) -> None:
        self.dataset = dataset
        self.csv_path = dataset.csv_path
        self.pcyl_series = dataset.pcyl_series
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
            dataset = prepare_viewer_dataset(csv_path, self.cycle_block_size)
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
        q1_cycle = self.q1_series.cycle_lookup.get(cycle)
        pcyl_block = self.pcyl_series.block_lookup.get(block_index)
        q1_block = self.q1_series.block_lookup.get(block_index)

        if pcyl_cycle is not None:
            self.pcyl_curve.setData(pcyl_cycle.x, pcyl_cycle.y)
        else:
            self.pcyl_curve.setData([], [])

        if q1_cycle is not None:
            self.q1_curve.setData(q1_cycle.x, q1_cycle.y)
        else:
            self.q1_curve.setData([], [])

        if self.show_block_mean and pcyl_block is not None:
            self.pcyl_block_curve.setData(pcyl_block.x, pcyl_block.y)
            self.pcyl_block_curve.show()
        else:
            self.pcyl_block_curve.hide()

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
        self.q1_plot.setTitle(f"Q_1 - Cycle {cycle} (block {block_label})")
        self.pmax_plot.setTitle(f"PMAX by cycle - selected cycle {cycle}")

        self._syncing = True
        self.slider.setValue(int(cycle))
        self.cycle_edit.setText(str(cycle))
        self._syncing = False
        self.current_cycle = cycle


def main() -> None:
    args = parse_args()
    csv_path = args.input.resolve()
    if not csv_path.exists():
        raise SystemExit(f"Input file not found: {csv_path}")
    dataset = prepare_viewer_dataset(csv_path, args.cycle_block_size)
    if args.no_show:
        print(f"[OK] Fast viewer prepared for {csv_path}")
        print(f"[OK] Available cycles: {len(dataset.available_cycles)}")
        print(f"[OK] Initial cycle: {args.initial_cycle}")
        print(f"[OK] Block mean overlay: {not args.hide_block_mean}")
        return

    app = QtWidgets.QApplication.instance() or QtWidgets.QApplication([])
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
