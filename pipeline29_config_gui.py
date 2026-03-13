from __future__ import annotations

import argparse
import fnmatch
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QHeaderView,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QStatusBar,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from pipeline29_config_backend import (
    DEFAULT_INSTRUMENT_COLUMNS,
    DEFAULT_MAPPING_COLUMNS,
    DEFAULT_PLOT_COLUMNS,
    DEFAULT_REPORTING_COLUMNS,
    Pipeline29ConfigBundle,
    bootstrap_text_config_from_excel,
    default_gui_state_path,
    default_preset_dir,
    default_text_config_dir,
    load_bundle_preset,
    load_gui_state,
    load_text_config_bundle,
    save_bundle_preset,
    save_gui_state,
    save_text_config_bundle,
    text_config_exists,
    validate_bundle,
)


SEARCHABLE_COLUMNS_BY_SECTION: Dict[str, set[str]] = {
    "Mappings": {"col_mean", "col_sd"},
    "Plots": {"x_col", "y_col", "yerr_col"},
}

INSTRUMENT_ZERO_DEFAULT_FIELDS = {"acc_pct", "digits", "lsd", "resolution"}
INSTRUMENT_SOURCE_DEFAULT = "User input"
PLOT_X_DEFAULTS = {"x_min": "0", "x_max": "55", "x_step": "5"}
PLOT_Y_AUTOSCALE_FIELDS = {"y_min", "y_max", "y_step"}

DEFAULT_FIELD_SPECS_BY_SECTION: Dict[str, List[Dict[str, Any]]] = {
    "Mappings": [
        {"name": "key", "kind": "text"},
        {"name": "col_mean", "kind": "variable"},
        {"name": "col_sd", "kind": "variable"},
        {"name": "unit", "kind": "text"},
        {"name": "notes", "kind": "text"},
    ],
    "Instruments": [
        {"name": "key", "kind": "mapping_key_combo"},
        {"name": "component", "kind": "text"},
        {"name": "dist", "kind": "combo", "options": ["rect", "normal", "triangular"]},
        {"name": "range_min", "kind": "text"},
        {"name": "range_max", "kind": "text"},
        {"name": "acc_abs", "kind": "text"},
        {"name": "acc_pct", "kind": "text"},
        {"name": "digits", "kind": "text"},
        {"name": "lsd", "kind": "text"},
        {"name": "resolution", "kind": "text"},
        {"name": "source", "kind": "source_combo"},
        {"name": "notes", "kind": "text"},
        {"name": "setting_param", "kind": "text"},
        {"name": "setting_value", "kind": "text"},
    ],
    "Plots": [
        {"name": "enabled", "kind": "combo", "options": ["1", "0"]},
        {"name": "plot_type", "kind": "combo", "options": ["all_fuels_yx", "all_fuels_xy", "all_fuels_labels", "kibox_all"]},
        {"name": "filename", "kind": "text"},
        {"name": "title", "kind": "text"},
        {"name": "x_col", "kind": "variable"},
        {"name": "y_col", "kind": "variable"},
        {"name": "yerr_col", "kind": "variable"},
        {"name": "show_uncertainty", "kind": "combo", "options": ["auto", "on", "off"]},
        {"name": "x_label", "kind": "text"},
        {"name": "y_label", "kind": "text"},
        {"name": "x_min", "kind": "text"},
        {"name": "x_max", "kind": "text"},
        {"name": "x_step", "kind": "text"},
        {"name": "y_min", "kind": "text"},
        {"name": "y_max", "kind": "text"},
        {"name": "y_step", "kind": "text"},
        {"name": "y_tol_plus", "kind": "text"},
        {"name": "y_tol_minus", "kind": "text"},
        {"name": "filter_h2o_list", "kind": "text"},
        {"name": "label_variant", "kind": "combo", "options": ["box", "inline", "none"]},
        {"name": "notes", "kind": "text"},
    ],
}


def _infer_sd_from_mean(col_mean: str) -> str:
    text = str(col_mean or "").strip()
    if not text:
        return ""
    replacements = [
        ("_mean_mean_of_windows", "_sd_of_windows"),
        ("_mean_of_windows", "_sd_of_windows"),
        ("_mean", "_sd"),
    ]
    for old, new in replacements:
        if old in text:
            return text.replace(old, new)
    return ""


def _safe_name(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    slug = re.sub(r"[^A-Za-z0-9]+", "_", raw).strip("_")
    return slug or "plot"


def _default_plot_filename(x_col: str, y_col: str) -> str:
    x_name = _safe_name(x_col)
    y_name = _safe_name(y_col)
    if not y_name:
        return ""
    if not x_name:
        return f"{y_name}.png"
    return f"{y_name}_vs_{x_name}_all.png"


def _default_plot_title(x_col: str, y_col: str) -> str:
    x_text = str(x_col or "").strip()
    y_text = str(y_col or "").strip()
    if not x_text or not y_text:
        return ""
    return f"{y_text} vs {x_text} (all fuels)"


def _load_variable_catalog_from_file(path: Path) -> List[str]:
    if not path.exists() or not path.is_file():
        return []

    suffix = path.suffix.lower()
    columns: List[str] = []

    if suffix in {".xlsx", ".xlsm", ".xls"}:
        xf = pd.ExcelFile(path)
        seen: set[str] = set()
        for sheet_name in xf.sheet_names:
            try:
                frame = pd.read_excel(path, sheet_name=sheet_name, nrows=0)
            except Exception:
                continue
            for column in frame.columns.tolist():
                text = str(column).strip()
                if not text or text in seen:
                    continue
                seen.add(text)
                columns.append(text)
        return columns

    if suffix == ".csv":
        for encoding in ("utf-8-sig", "utf-8", "latin-1"):
            try:
                frame = pd.read_csv(path, nrows=0, sep=None, engine="python", encoding=encoding)
                return [str(column).strip() for column in frame.columns.tolist() if str(column).strip()]
            except Exception:
                continue
    return []


def _build_source_catalog_from_records(records: List[Dict[str, str]]) -> Dict[str, str]:
    catalog: Dict[str, str] = {
        "User input": "Manual assumption entered by the user. Typical use: +/- limit informed directly for the sensor.",
        "ASTM E230 / ANSI MC96.1 summary": "Thermocouple standard-grade tolerance reference used for K/T sensor uncertainty.",
        "NI 9213 datasheet Fig. 3 (approx.)": "Approximate NI 9213 module uncertainty for Type K based on the datasheet curves.",
        "NI 9213 datasheet Fig. 4 (approx.)": "Approximate NI 9213 module uncertainty for Type T based on the datasheet curves.",
    }

    grouped: Dict[str, List[Dict[str, str]]] = {}
    for record in records:
        source = str(record.get("source", "")).strip()
        if not source:
            continue
        grouped.setdefault(source, []).append(record)

    for source, source_records in grouped.items():
        components = sorted({str(row.get("component", "")).strip() for row in source_records if str(row.get("component", "")).strip()})
        notes = sorted({str(row.get("notes", "")).strip() for row in source_records if str(row.get("notes", "")).strip()})
        parts: List[str] = []
        if components:
            preview = ", ".join(components[:3])
            if len(components) > 3:
                preview += ", ..."
            parts.append(f"Components: {preview}")
        if notes:
            parts.append(notes[0])
        if parts:
            catalog[source] = " ".join(parts)

    return catalog


class VariableSelectorDialog(QDialog):
    def __init__(
        self,
        *,
        title: str,
        variable_names: List[str],
        current_value: str = "",
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self.variable_names = sorted({name for name in variable_names if str(name).strip()}, key=str.lower)
        self.selected_value = current_value.strip()

        self.setWindowTitle(title)
        self.resize(760, 520)

        root = QVBoxLayout(self)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(10)

        root.addWidget(QLabel("Search accepts wildcard. Example: *T_* or U_*"))

        self.search_edit = QLineEdit(current_value)
        self.search_edit.setPlaceholderText("Type wildcard or part of the variable name")
        root.addWidget(self.search_edit)

        self.list_widget = QListWidget(self)
        root.addWidget(self.list_widget, 1)

        self.status_label = QLabel("")
        root.addWidget(self.status_label)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel, parent=self)
        root.addWidget(buttons)

        buttons.accepted.connect(self._accept_selection)
        buttons.rejected.connect(self.reject)
        self.search_edit.textChanged.connect(self._refresh_list)
        self.list_widget.itemDoubleClicked.connect(lambda _item: self._accept_selection())

        self._refresh_list()
        self.search_edit.setFocus()
        self.search_edit.selectAll()

    def _filtered_variables(self) -> List[str]:
        raw = self.search_edit.text().strip()
        if not raw:
            return self.variable_names
        pattern = raw
        if not any(ch in pattern for ch in "*?[]"):
            pattern = f"*{pattern}*"
        pattern_low = pattern.lower()
        return [name for name in self.variable_names if fnmatch.fnmatch(name.lower(), pattern_low)]

    def _refresh_list(self) -> None:
        items = self._filtered_variables()
        self.list_widget.clear()
        for name in items:
            self.list_widget.addItem(QListWidgetItem(name))
        if items:
            self.list_widget.setCurrentRow(0)
        self.status_label.setText(f"{len(items)} variable(s) matched")

    def _accept_selection(self) -> None:
        item = self.list_widget.currentItem()
        if item is not None:
            self.selected_value = item.text().strip()
            self.accept()
            return
        text = self.search_edit.text().strip()
        if text:
            self.selected_value = text
            self.accept()
            return
        QMessageBox.warning(self, "Pipeline 29", "Select one variable or type a value.")


class ConfigRowDialog(QDialog):
    def __init__(
        self,
        *,
        section_title: str,
        field_specs: List[Dict[str, Any]],
        initial_values: Optional[Dict[str, Any]] = None,
        variable_catalog_provider: Optional[Callable[[], List[str]]] = None,
        mapping_key_provider: Optional[Callable[[], List[str]]] = None,
        source_catalog_provider: Optional[Callable[[], Dict[str, str]]] = None,
        status_callback: Optional[Callable[[str], None]] = None,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self.section_title = section_title
        self.field_specs = field_specs
        self.initial_values = self._prepare_initial_values(initial_values or {})
        self.variable_catalog_provider = variable_catalog_provider
        self.mapping_key_provider = mapping_key_provider
        self.source_catalog_provider = source_catalog_provider
        self.status_callback = status_callback
        self.widgets: Dict[str, Any] = {}
        self.info_labels: Dict[str, QLabel] = {}
        self._last_auto_sd = ""
        self._last_auto_filename = ""
        self._last_auto_title = ""

        self.setWindowTitle(f"{section_title} helper")
        self.resize(640 if section_title == "Plots" else 560, 720 if section_title == "Plots" else 620)

        root = QVBoxLayout(self)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(10)

        root.addWidget(QLabel(f"Configure a new row for {section_title}"))
        if section_title == "Plots":
            root.addWidget(QLabel("X defaults: min=0, max=55, step=5. Y default: autoscale. Leave y_min / y_max / y_step blank to keep autoscale."))

        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        root.addWidget(scroll, 1)

        content = QWidget(self)
        scroll.setWidget(content)
        form = QFormLayout(content)
        form.setContentsMargins(8, 8, 8, 8)
        form.setSpacing(10)

        for spec in field_specs:
            field_name = spec["name"]
            label = QLabel(field_name)
            editor = self._build_editor(spec, self.initial_values.get(field_name, ""))
            form.addRow(label, editor)
            extra = self._field_extra_label(field_name)
            if extra is not None:
                form.addRow(QLabel(""), extra)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel, parent=self)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)
        self._after_build()

    def _prepare_initial_values(self, values: Dict[str, Any]) -> Dict[str, Any]:
        prepared = dict(values or {})
        if self.section_title == "Instruments":
            for field in INSTRUMENT_ZERO_DEFAULT_FIELDS:
                if not str(prepared.get(field, "")).strip():
                    prepared[field] = "0"
            if not str(prepared.get("source", "")).strip():
                prepared["source"] = INSTRUMENT_SOURCE_DEFAULT
        if self.section_title == "Plots":
            if not str(prepared.get("enabled", "")).strip():
                prepared["enabled"] = "1"
            if not str(prepared.get("plot_type", "")).strip():
                prepared["plot_type"] = "all_fuels_yx"
            if not str(prepared.get("show_uncertainty", "")).strip():
                prepared["show_uncertainty"] = "auto"
            if not str(prepared.get("x_col", "")).strip():
                prepared["x_col"] = "Load_kW"
            if not str(prepared.get("x_label", "")).strip():
                prepared["x_label"] = "Power (kW)"
            for field, default_value in PLOT_X_DEFAULTS.items():
                if not str(prepared.get(field, "")).strip():
                    prepared[field] = default_value
        return prepared

    def _build_editor(self, spec: Dict[str, Any], value: Any) -> QWidget:
        kind = str(spec.get("kind", "text"))
        field_name = str(spec.get("name", ""))
        value_text = "" if value is None else str(value)

        if kind == "combo":
            combo = QComboBox(self)
            combo.setEditable(True)
            options = [str(option) for option in spec.get("options", [])]
            if value_text and value_text not in options:
                options = options + [value_text]
            combo.addItems(options)
            combo.setCurrentText(value_text)
            self.widgets[field_name] = combo
            return combo

        if kind == "source_combo":
            combo = QComboBox(self)
            combo.setEditable(True)
            options = []
            if self.source_catalog_provider is not None:
                options = sorted(self.source_catalog_provider().keys(), key=str.lower)
            if value_text and value_text not in options:
                options.append(value_text)
            combo.addItems(options)
            combo.setCurrentText(value_text or INSTRUMENT_SOURCE_DEFAULT)
            self.widgets[field_name] = combo
            combo.currentTextChanged.connect(lambda _text: self._update_source_description())
            return combo

        if kind == "mapping_key_combo":
            combo = QComboBox(self)
            combo.setEditable(True)
            options = self.mapping_key_provider() if self.mapping_key_provider is not None else []
            options = [str(option) for option in options if str(option).strip()]
            options = sorted(dict.fromkeys(options), key=str.lower)
            if value_text and value_text not in options:
                options.append(value_text)
            combo.addItems(options)
            combo.setCurrentText(value_text)
            self.widgets[field_name] = combo
            return combo

        if kind == "variable":
            container = QWidget(self)
            layout = QHBoxLayout(container)
            layout.setContentsMargins(0, 0, 0, 0)
            layout.setSpacing(6)
            line_edit = QLineEdit(value_text, container)
            pick_button = QPushButton("Pick", container)
            pick_button.clicked.connect(lambda _checked=False, target=line_edit, field=field_name: self._pick_variable(field, target))
            layout.addWidget(line_edit, 1)
            layout.addWidget(pick_button)
            self.widgets[field_name] = line_edit
            return container

        line_edit = QLineEdit(value_text, self)
        self.widgets[field_name] = line_edit
        return line_edit

    def _field_extra_label(self, field_name: str) -> Optional[QLabel]:
        if self.section_title == "Instruments" and field_name == "acc_abs":
            label = QLabel("Absolute accuracy limit. Example: enter 2.93 for a sensor specified as +/-2.93 kPa.")
            label.setWordWrap(True)
            return label
        if self.section_title == "Instruments" and field_name == "source":
            label = QLabel("")
            label.setWordWrap(True)
            self.info_labels["source"] = label
            return label
        if self.section_title == "Instruments" and field_name == "resolution":
            label = QLabel("If acc_pct, digits, lsd or resolution are left blank, the helper stores 0 by default.")
            label.setWordWrap(True)
            return label
        if self.section_title == "Plots" and field_name == "y_step":
            label = QLabel("Y axis defaults to autoscale. Leave y_min, y_max and y_step blank if you want autoscale.")
            label.setWordWrap(True)
            return label
        return None

    def _after_build(self) -> None:
        if self.section_title == "Mappings":
            mean_widget = self.widgets.get("col_mean")
            if isinstance(mean_widget, QLineEdit):
                mean_widget.textChanged.connect(lambda _text: self._maybe_sync_mapping_sd())
            self._maybe_sync_mapping_sd(force_if_empty=True)
        if self.section_title == "Plots":
            for field_name in ("x_col", "y_col"):
                widget = self.widgets.get(field_name)
                if isinstance(widget, QLineEdit):
                    widget.textChanged.connect(lambda _text: self._maybe_sync_plot_defaults())
            self._maybe_sync_plot_defaults(force_if_empty=True)
        if self.section_title == "Instruments":
            self._update_source_description()

    def _maybe_sync_mapping_sd(self, force_if_empty: bool = False) -> None:
        mean_widget = self.widgets.get("col_mean")
        sd_widget = self.widgets.get("col_sd")
        if not isinstance(mean_widget, QLineEdit) or not isinstance(sd_widget, QLineEdit):
            return
        current_mean = mean_widget.text().strip()
        candidate = _infer_sd_from_mean(current_mean)
        if not candidate:
            return
        current_sd = sd_widget.text().strip()
        if force_if_empty or not current_sd or current_sd == self._last_auto_sd:
            sd_widget.setText(candidate)
            self._last_auto_sd = candidate

    def _maybe_sync_plot_defaults(self, force_if_empty: bool = False) -> None:
        x_widget = self.widgets.get("x_col")
        y_widget = self.widgets.get("y_col")
        filename_widget = self.widgets.get("filename")
        title_widget = self.widgets.get("title")
        if not isinstance(x_widget, QLineEdit) or not isinstance(y_widget, QLineEdit):
            return
        x_col = x_widget.text().strip()
        y_col = y_widget.text().strip()

        auto_filename = _default_plot_filename(x_col, y_col)
        if isinstance(filename_widget, QLineEdit) and auto_filename:
            current = filename_widget.text().strip()
            if force_if_empty or not current or current == self._last_auto_filename:
                filename_widget.setText(auto_filename)
                self._last_auto_filename = auto_filename

        auto_title = _default_plot_title(x_col, y_col)
        if isinstance(title_widget, QLineEdit) and auto_title:
            current = title_widget.text().strip()
            if force_if_empty or not current or current == self._last_auto_title:
                title_widget.setText(auto_title)
                self._last_auto_title = auto_title

    def _update_source_description(self) -> None:
        label = self.info_labels.get("source")
        widget = self.widgets.get("source")
        if label is None or not isinstance(widget, QComboBox):
            return
        source = widget.currentText().strip() or INSTRUMENT_SOURCE_DEFAULT
        catalog = self.source_catalog_provider() if self.source_catalog_provider is not None else {}
        desc = catalog.get(source, "Custom source. Use it to document where the equipment uncertainty came from.")
        label.setText(f"Source description: {desc}")

    def _pick_variable(self, field_name: str, target: QLineEdit) -> None:
        variable_names = self.variable_catalog_provider() if self.variable_catalog_provider is not None else []
        if not variable_names:
            if self.status_callback is not None:
                self.status_callback("No variable catalog loaded. Choose a source file first.")
            QMessageBox.warning(
                self,
                "Pipeline 29",
                "No variable catalog loaded.\nChoose a variable source file first.",
            )
            return
        dialog = VariableSelectorDialog(
            title=f"{self.section_title} - select {field_name}",
            variable_names=variable_names,
            current_value=target.text().strip(),
            parent=self,
        )
        if dialog.exec() == QDialog.Accepted:
            target.setText(dialog.selected_value)
            if self.section_title == "Mappings" and field_name == "col_mean":
                self._maybe_sync_mapping_sd()
            if self.section_title == "Plots" and field_name in {"x_col", "y_col"}:
                self._maybe_sync_plot_defaults(force_if_empty=True)

    def values(self) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for field_name, widget in self.widgets.items():
            if isinstance(widget, QComboBox):
                out[field_name] = widget.currentText().strip()
            elif isinstance(widget, QLineEdit):
                out[field_name] = widget.text().strip()
            else:
                out[field_name] = ""
        if self.section_title == "Instruments":
            for field in INSTRUMENT_ZERO_DEFAULT_FIELDS:
                if not out.get(field, "").strip():
                    out[field] = "0"
            if not out.get("source", "").strip():
                out["source"] = INSTRUMENT_SOURCE_DEFAULT
        return out


class EditableTableSection(QWidget):
    def __init__(
        self,
        title: str,
        columns: List[str],
        *,
        searchable_columns: Optional[set[str]] = None,
        variable_catalog_provider: Optional[Callable[[], List[str]]] = None,
        mapping_key_provider: Optional[Callable[[], List[str]]] = None,
        status_callback: Optional[Callable[[str], None]] = None,
        add_row_dialog_factory: Optional[Callable[[Optional[Dict[str, Any]]], Optional[Dict[str, str]]]] = None,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self.title = title
        self.columns = columns
        self.searchable_columns = searchable_columns or set()
        self.variable_catalog_provider = variable_catalog_provider
        self.mapping_key_provider = mapping_key_provider
        self.status_callback = status_callback
        self.add_row_dialog_factory = add_row_dialog_factory

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        toolbar = QHBoxLayout()
        toolbar.addWidget(QLabel(title))
        if self.searchable_columns:
            toolbar.addWidget(QLabel("Double-click searchable cells to pick a variable"))
        toolbar.addStretch(1)

        self.btn_add = QPushButton("Add row")
        self.btn_duplicate = QPushButton("Duplicate selected")
        self.btn_remove = QPushButton("Remove selected")
        toolbar.addWidget(self.btn_add)
        toolbar.addWidget(self.btn_duplicate)
        toolbar.addWidget(self.btn_remove)
        layout.addLayout(toolbar)

        self.table = QTableWidget(0, len(columns), self)
        self.table.setHorizontalHeaderLabels(columns)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)
        header.setStretchLastSection(True)
        layout.addWidget(self.table, 1)

        self.btn_add.clicked.connect(self._prompt_add_row)
        self.btn_duplicate.clicked.connect(self.duplicate_selected_rows)
        self.btn_remove.clicked.connect(self.remove_selected_rows)
        self.table.cellDoubleClicked.connect(self._handle_cell_double_click)

    def _insert_row(self, values: Optional[Dict[str, Any]] = None) -> None:
        row = self.table.rowCount()
        self.table.insertRow(row)
        values = values or {}
        for col_idx, column in enumerate(self.columns):
            text = "" if values.get(column) is None else str(values.get(column))
            self.table.setItem(row, col_idx, QTableWidgetItem(text))

    def _prompt_add_row(self) -> None:
        values: Optional[Dict[str, Any]] = None
        if self.add_row_dialog_factory is not None:
            values = self.add_row_dialog_factory(None)
            if values is None:
                return
        self._insert_row(values)

    def load_records(self, records: List[Dict[str, Any]]) -> None:
        self.table.setRowCount(0)
        for record in records:
            self._insert_row(record)

    def remove_selected_rows(self) -> None:
        rows = sorted({item.row() for item in self.table.selectedItems()}, reverse=True)
        for row in rows:
            self.table.removeRow(row)

    def duplicate_selected_rows(self) -> None:
        rows = sorted({item.row() for item in self.table.selectedItems()})
        for row in rows:
            self._insert_row(self.record_at(row))

    def record_at(self, row: int) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for col_idx, column in enumerate(self.columns):
            item = self.table.item(row, col_idx)
            out[column] = "" if item is None else item.text().strip()
        return out

    def records(self) -> List[Dict[str, str]]:
        out: List[Dict[str, str]] = []
        for row in range(self.table.rowCount()):
            record = self.record_at(row)
            if any(str(value).strip() for value in record.values()):
                out.append(record)
        return out

    def _handle_cell_double_click(self, row: int, col_idx: int) -> None:
        column = self.columns[col_idx]
        if column not in self.searchable_columns or self.variable_catalog_provider is None:
            return

        variable_names = self.variable_catalog_provider()
        if not variable_names:
            if self.status_callback is not None:
                self.status_callback("No variable catalog loaded. Choose a source file first.")
            QMessageBox.warning(
                self,
                "Pipeline 29",
                "No variable catalog loaded.\nChoose a variable source file first.",
            )
            return

        item = self.table.item(row, col_idx)
        current_value = "" if item is None else item.text().strip()
        dialog = VariableSelectorDialog(
            title=f"{self.title} - select {column}",
            variable_names=variable_names,
            current_value=current_value,
            parent=self,
        )
        if dialog.exec() != QDialog.Accepted:
            return
        if item is None:
            item = QTableWidgetItem("")
            self.table.setItem(row, col_idx, item)
        item.setText(dialog.selected_value)
        if self.status_callback is not None:
            self.status_callback(f"{self.title}.{column} updated to '{dialog.selected_value}'")


class Pipeline29ConfigEditor(QMainWindow):
    def __init__(self, *, base_dir: Path, config_dir: Path, excel_path: Path) -> None:
        super().__init__()
        self.base_dir = base_dir
        self.last_preset_path = ""
        self.variable_catalog: List[str] = []

        default_config_dir = config_dir.resolve()
        default_excel_path = excel_path.resolve()
        state = load_gui_state(default_gui_state_path())

        state_config_dir = str(state.get("config_dir", "")).strip()
        state_excel_path = str(state.get("excel_path", "")).strip()
        state_variable_source = str(state.get("variable_source_path", "")).strip()
        self.last_preset_path = str(state.get("last_preset_path", "")).strip()

        self.config_dir = Path(state_config_dir).expanduser().resolve() if state_config_dir else default_config_dir
        self.excel_path = Path(state_excel_path).expanduser().resolve() if state_excel_path else default_excel_path
        self.variable_source_path = (
            Path(state_variable_source).expanduser().resolve()
            if state_variable_source
            else (self.base_dir / "out" / "lv_kpis_clean.xlsx").resolve()
        )

        self.setWindowTitle("Pipeline 29 Config Editor")
        self.resize(1700, 980)

        central = QWidget(self)
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(10)

        config_row = QHBoxLayout()
        config_row.addWidget(QLabel("Text config dir"))
        self.config_dir_edit = QLineEdit(str(self.config_dir))
        config_row.addWidget(self.config_dir_edit, 1)
        self.btn_browse_config_dir = QPushButton("Browse")
        config_row.addWidget(self.btn_browse_config_dir)
        root.addLayout(config_row)

        excel_row = QHBoxLayout()
        excel_row.addWidget(QLabel("Excel rev3"))
        self.excel_path_edit = QLineEdit(str(self.excel_path))
        excel_row.addWidget(self.excel_path_edit, 1)
        self.btn_browse_excel = QPushButton("Browse")
        excel_row.addWidget(self.btn_browse_excel)
        root.addLayout(excel_row)

        variable_row = QHBoxLayout()
        variable_row.addWidget(QLabel("Variable source"))
        self.variable_source_edit = QLineEdit(str(self.variable_source_path))
        variable_row.addWidget(self.variable_source_edit, 1)
        self.btn_browse_variable_source = QPushButton("Browse")
        self.btn_reload_variable_catalog = QPushButton("Reload catalog")
        variable_row.addWidget(self.btn_browse_variable_source)
        variable_row.addWidget(self.btn_reload_variable_catalog)
        root.addLayout(variable_row)

        actions = QHBoxLayout()
        self.btn_reload_text = QPushButton("Reload text")
        self.btn_import_excel = QPushButton("Import Excel -> text")
        self.btn_save = QPushButton("Save")
        self.btn_save_as = QPushButton("Save As")
        self.btn_validate = QPushButton("Validate")
        self.btn_save_preset = QPushButton("Save preset")
        self.btn_load_preset = QPushButton("Load preset")
        actions.addWidget(self.btn_reload_text)
        actions.addWidget(self.btn_import_excel)
        actions.addWidget(self.btn_save)
        actions.addWidget(self.btn_save_as)
        actions.addWidget(self.btn_validate)
        actions.addWidget(self.btn_save_preset)
        actions.addWidget(self.btn_load_preset)
        actions.addStretch(1)
        root.addLayout(actions)

        self.tabs = QTabWidget(self)
        root.addWidget(self.tabs, 1)

        self.defaults_table = EditableTableSection("Defaults", ["param", "value"], status_callback=self._show_status)
        self.data_quality_table = EditableTableSection("Data Quality", ["param", "value"], status_callback=self._show_status)
        self.mappings_table = EditableTableSection(
            "Mappings",
            DEFAULT_MAPPING_COLUMNS,
            searchable_columns=SEARCHABLE_COLUMNS_BY_SECTION.get("Mappings", set()),
            variable_catalog_provider=self._available_variable_catalog,
            status_callback=self._show_status,
            add_row_dialog_factory=lambda initial=None: self._open_row_helper("Mappings", DEFAULT_MAPPING_COLUMNS, initial),
        )
        self.instruments_table = EditableTableSection(
            "Instruments",
            DEFAULT_INSTRUMENT_COLUMNS,
            mapping_key_provider=self._current_mapping_keys,
            status_callback=self._show_status,
            add_row_dialog_factory=lambda initial=None: self._open_row_helper("Instruments", DEFAULT_INSTRUMENT_COLUMNS, initial),
        )
        self.reporting_table = EditableTableSection("Reporting_Rounding", DEFAULT_REPORTING_COLUMNS, status_callback=self._show_status)
        self.plots_table = EditableTableSection(
            "Plots",
            DEFAULT_PLOT_COLUMNS,
            searchable_columns=SEARCHABLE_COLUMNS_BY_SECTION.get("Plots", set()),
            variable_catalog_provider=self._available_variable_catalog,
            status_callback=self._show_status,
            add_row_dialog_factory=lambda initial=None: self._open_row_helper("Plots", DEFAULT_PLOT_COLUMNS, initial),
        )

        self.tabs.addTab(self.defaults_table, "Defaults")
        self.tabs.addTab(self.data_quality_table, "Data Quality")
        self.tabs.addTab(self.mappings_table, "Mappings")
        self.tabs.addTab(self.instruments_table, "Instruments")
        self.tabs.addTab(self.reporting_table, "Reporting")
        self.tabs.addTab(self.plots_table, "Plots")

        self.status = QStatusBar(self)
        self.setStatusBar(self.status)

        self.btn_browse_config_dir.clicked.connect(self._choose_config_dir)
        self.btn_browse_excel.clicked.connect(self._choose_excel_path)
        self.btn_browse_variable_source.clicked.connect(self._choose_variable_source_path)
        self.btn_reload_variable_catalog.clicked.connect(self.reload_variable_catalog)
        self.btn_reload_text.clicked.connect(self.reload_text_bundle)
        self.btn_import_excel.clicked.connect(self.import_from_excel)
        self.btn_save.clicked.connect(self.save_text_bundle)
        self.btn_save_as.clicked.connect(self.save_text_bundle_as)
        self.btn_validate.clicked.connect(self.validate_current_bundle)
        self.btn_save_preset.clicked.connect(self.save_preset)
        self.btn_load_preset.clicked.connect(self.load_preset)

        self._load_initial_bundle()
        self.reload_variable_catalog(show_message=False)

    def _current_config_dir(self) -> Path:
        raw = self.config_dir_edit.text().strip()
        if raw:
            return Path(raw).expanduser().resolve()
        return default_text_config_dir(self.base_dir)

    def _current_excel_path(self) -> Path:
        raw = self.excel_path_edit.text().strip()
        if raw:
            return Path(raw).expanduser().resolve()
        return (self.base_dir / "config" / "config_incertezas_rev3.xlsx").resolve()

    def _current_variable_source_path(self) -> Path:
        raw = self.variable_source_edit.text().strip()
        if raw:
            return Path(raw).expanduser().resolve()
        return (self.base_dir / "out" / "lv_kpis_clean.xlsx").resolve()

    def _choose_config_dir(self) -> None:
        selected = QFileDialog.getExistingDirectory(self, "Select pipeline29 text config dir", str(self._current_config_dir()))
        if selected:
            self.config_dir_edit.setText(selected)

    def _choose_excel_path(self) -> None:
        selected, _ = QFileDialog.getOpenFileName(
            self,
            "Select config_incertezas_rev3.xlsx",
            str(self._current_excel_path()),
            "Excel (*.xlsx)",
        )
        if selected:
            self.excel_path_edit.setText(selected)

    def _choose_variable_source_path(self) -> None:
        selected, _ = QFileDialog.getOpenFileName(
            self,
            "Select variable source file",
            str(self._current_variable_source_path()),
            "Data files (*.xlsx *.xlsm *.xls *.csv)",
        )
        if selected:
            self.variable_source_edit.setText(selected)
            self.reload_variable_catalog(show_message=True)

    def _load_initial_bundle(self) -> None:
        config_dir = self._current_config_dir()
        excel_path = self._current_excel_path()
        if text_config_exists(config_dir):
            bundle = load_text_config_bundle(config_dir)
            self._load_bundle(bundle)
            self._show_status(f"Loaded text config from {config_dir}")
            return
        if excel_path.exists():
            bundle = bootstrap_text_config_from_excel(excel_path, config_dir)
            self._load_bundle(bundle)
            self._show_status(f"Bootstrapped text config from {excel_path}")
            return
        self._load_bundle(self._empty_bundle())
        self._show_status("No text config or Excel found. Started with empty tables.")

    def _empty_bundle(self) -> Pipeline29ConfigBundle:
        return Pipeline29ConfigBundle(
            mappings={},
            instruments_df=pd.DataFrame(columns=DEFAULT_INSTRUMENT_COLUMNS),
            reporting_df=pd.DataFrame(columns=DEFAULT_REPORTING_COLUMNS),
            plots_df=pd.DataFrame(columns=DEFAULT_PLOT_COLUMNS),
            data_quality_cfg={},
            defaults_cfg={},
            source_kind="text",
            source_path=self._current_config_dir(),
            text_dir=self._current_config_dir(),
        )

    def _load_bundle(self, bundle: Pipeline29ConfigBundle) -> None:
        defaults_records = [{"param": key, "value": value} for key, value in bundle.defaults_cfg.items()]
        data_quality_records = [{"param": key, "value": value} for key, value in bundle.data_quality_cfg.items()]
        mappings_records: List[Dict[str, Any]] = []
        for key, spec in bundle.mappings.items():
            mappings_records.append(
                {
                    "key": key,
                    "col_mean": spec.get("mean", ""),
                    "col_sd": spec.get("sd", ""),
                    "unit": spec.get("unit", ""),
                    "notes": spec.get("notes", ""),
                }
            )

        self.defaults_table.load_records(defaults_records)
        self.data_quality_table.load_records(data_quality_records)
        self.mappings_table.load_records(mappings_records)
        self.instruments_table.load_records(bundle.instruments_df.to_dict(orient="records"))
        self.reporting_table.load_records(bundle.reporting_df.to_dict(orient="records"))
        self.plots_table.load_records(bundle.plots_df.to_dict(orient="records"))

    def _available_variable_catalog(self) -> List[str]:
        names = {name for name in self.variable_catalog if str(name).strip()}
        for table_section, columns in (
            (self.mappings_table, SEARCHABLE_COLUMNS_BY_SECTION.get("Mappings", set())),
            (self.plots_table, SEARCHABLE_COLUMNS_BY_SECTION.get("Plots", set())),
        ):
            for record in table_section.records():
                for column in columns:
                    text = str(record.get(column, "")).strip()
                    if text:
                        names.add(text)
        return sorted(names, key=str.lower)

    def _current_mapping_keys(self) -> List[str]:
        keys: List[str] = []
        for record in self.mappings_table.records():
            key = str(record.get("key", "")).strip()
            if key:
                keys.append(key)
        return sorted(dict.fromkeys(keys), key=str.lower)

    def _current_source_catalog(self) -> Dict[str, str]:
        return _build_source_catalog_from_records(self.instruments_table.records())

    def _open_row_helper(
        self,
        section_title: str,
        columns: List[str],
        initial_values: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, str]]:
        field_specs = DEFAULT_FIELD_SPECS_BY_SECTION.get(
            section_title,
            [{"name": column, "kind": "text"} for column in columns],
        )
        dialog = ConfigRowDialog(
            section_title=section_title,
            field_specs=field_specs,
            initial_values=initial_values,
            variable_catalog_provider=self._available_variable_catalog,
            mapping_key_provider=self._current_mapping_keys,
            source_catalog_provider=self._current_source_catalog,
            status_callback=self._show_status,
            parent=self,
        )
        if dialog.exec() != QDialog.Accepted:
            return None
        values = dialog.values()
        for column in columns:
            values.setdefault(column, "")
        return values

    def _bundle_from_ui(self) -> Tuple[Pipeline29ConfigBundle, List[str]]:
        errors: List[str] = []

        defaults_cfg: Dict[str, str] = {}
        for row in self.defaults_table.records():
            key = row.get("param", "").strip()
            if not key:
                continue
            defaults_cfg[key] = row.get("value", "").strip()

        data_quality_cfg: Dict[str, float] = {}
        for row in self.data_quality_table.records():
            key = row.get("param", "").strip()
            value_txt = row.get("value", "").strip()
            if not key or not value_txt:
                continue
            try:
                data_quality_cfg[key] = float(value_txt.replace(",", "."))
            except Exception:
                errors.append(f"Data quality '{key}' precisa ser numerico. Valor atual: '{value_txt}'")

        mappings: Dict[str, Dict[str, str]] = {}
        for row in self.mappings_table.records():
            key = row.get("key", "").strip()
            if not key:
                continue
            mappings[key] = {
                "mean": row.get("col_mean", "").strip(),
                "sd": row.get("col_sd", "").strip(),
                "unit": row.get("unit", "").strip(),
                "notes": row.get("notes", "").strip(),
            }

        bundle = Pipeline29ConfigBundle(
            mappings=mappings,
            instruments_df=pd.DataFrame(self.instruments_table.records(), columns=DEFAULT_INSTRUMENT_COLUMNS),
            reporting_df=pd.DataFrame(self.reporting_table.records(), columns=DEFAULT_REPORTING_COLUMNS),
            plots_df=pd.DataFrame(self.plots_table.records(), columns=DEFAULT_PLOT_COLUMNS),
            data_quality_cfg=data_quality_cfg,
            defaults_cfg=defaults_cfg,
            source_kind="text",
            source_path=self._current_config_dir(),
            text_dir=self._current_config_dir(),
        )
        errors.extend(validate_bundle(bundle))
        return bundle, errors

    def _show_status(self, message: str) -> None:
        self.status.showMessage(message, 12000)

    def reload_variable_catalog(self, *, show_message: bool = True) -> None:
        path = self._current_variable_source_path()
        self.variable_catalog = _load_variable_catalog_from_file(path)
        if show_message:
            if self.variable_catalog:
                self._show_status(f"Loaded {len(self.variable_catalog)} variable(s) from {path}")
            else:
                self._show_status(f"No variable catalog loaded from {path}")

    def reload_text_bundle(self) -> None:
        config_dir = self._current_config_dir()
        if not text_config_exists(config_dir):
            QMessageBox.warning(self, "Pipeline 29", f"No text config found in:\n{config_dir}")
            return
        bundle = load_text_config_bundle(config_dir)
        self._load_bundle(bundle)
        self._show_status(f"Reloaded text config from {config_dir}")

    def import_from_excel(self) -> None:
        excel_path = self._current_excel_path()
        config_dir = self._current_config_dir()
        if not excel_path.exists():
            QMessageBox.critical(self, "Pipeline 29", f"Excel file not found:\n{excel_path}")
            return
        bundle = bootstrap_text_config_from_excel(excel_path, config_dir)
        self._load_bundle(bundle)
        self._show_status(f"Imported Excel rev3 into {config_dir}")

    def validate_current_bundle(self) -> None:
        _bundle, errors = self._bundle_from_ui()
        if errors:
            QMessageBox.warning(self, "Pipeline 29", "\n".join(errors))
            self._show_status("Validation finished with errors.")
            return
        QMessageBox.information(self, "Pipeline 29", "Config validated successfully.")
        self._show_status("Config validated successfully.")

    def save_text_bundle(self) -> None:
        bundle, errors = self._bundle_from_ui()
        if errors:
            QMessageBox.warning(self, "Pipeline 29", "\n".join(errors))
            return
        config_dir = self._current_config_dir()
        saved = save_text_config_bundle(bundle, config_dir)
        self.config_dir_edit.setText(str(config_dir))
        self._load_bundle(saved)
        self._show_status(f"Saved text config to {config_dir}")

    def save_text_bundle_as(self) -> None:
        parent_dir = QFileDialog.getExistingDirectory(
            self,
            "Select parent directory for Save As",
            str(self._current_config_dir().parent),
        )
        if not parent_dir:
            return

        default_name = self._current_config_dir().name or "pipeline29_text"
        folder_name, ok = QInputDialog.getText(
            self,
            "Save As",
            "Config folder name:",
            text=default_name,
        )
        if not ok or not folder_name.strip():
            return

        target_dir = Path(parent_dir) / folder_name.strip()
        if target_dir.exists() and target_dir != self._current_config_dir():
            answer = QMessageBox.question(
                self,
                "Pipeline 29",
                f"Directory already exists:\n{target_dir}\n\nOverwrite files?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if answer != QMessageBox.Yes:
                return

        self.config_dir_edit.setText(str(target_dir))
        self.save_text_bundle()

    def save_preset(self) -> None:
        bundle, errors = self._bundle_from_ui()
        if errors:
            QMessageBox.warning(self, "Pipeline 29", "\n".join(errors))
            return
        preset_dir = default_preset_dir()
        default_name = Path(self.last_preset_path).name if self.last_preset_path else "pipeline29_preset.json"
        target, _ = QFileDialog.getSaveFileName(
            self,
            "Save preset",
            str(preset_dir / default_name),
            "JSON (*.json)",
        )
        if not target:
            return
        save_bundle_preset(bundle, Path(target))
        self.last_preset_path = target
        self._show_status(f"Preset saved to {target}")

    def load_preset(self) -> None:
        preset_dir = default_preset_dir()
        start_path = self.last_preset_path or str(preset_dir)
        selected, _ = QFileDialog.getOpenFileName(
            self,
            "Load preset",
            start_path,
            "JSON (*.json)",
        )
        if not selected:
            return
        bundle = load_bundle_preset(Path(selected))
        self._load_bundle(bundle)
        self.last_preset_path = selected
        self._show_status(f"Preset loaded from {selected}")

    def closeEvent(self, event) -> None:  # type: ignore[override]
        save_gui_state(
            {
                "config_dir": str(self._current_config_dir()),
                "excel_path": str(self._current_excel_path()),
                "variable_source_path": str(self._current_variable_source_path()),
                "last_preset_path": self.last_preset_path,
            },
            default_gui_state_path(),
        )
        super().closeEvent(event)


def launch_config_gui(*, base_dir: Path, config_dir: Optional[Path] = None, excel_path: Optional[Path] = None) -> int:
    app = QApplication.instance() or QApplication(["pipeline29-config-gui"])
    window = Pipeline29ConfigEditor(
        base_dir=base_dir,
        config_dir=(config_dir or default_text_config_dir(base_dir)).resolve(),
        excel_path=(excel_path or (base_dir / "config" / "config_incertezas_rev3.xlsx")).resolve(),
    )
    window.showMaximized()
    return app.exec()


def _parse_cli_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GUI da configuracao textual do pipeline29.")
    parser.add_argument("--base-dir", default="", help="Diretorio base do repo Processamentos.")
    parser.add_argument("--config-dir", default="", help="Diretorio da configuracao textual.")
    parser.add_argument("--excel-path", default="", help="Caminho para o config_incertezas_rev3.xlsx.")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_cli_args(argv)
    base_dir = Path(args.base_dir).expanduser().resolve() if args.base_dir else Path(__file__).resolve().parent
    config_dir = Path(args.config_dir).expanduser().resolve() if args.config_dir else default_text_config_dir(base_dir)
    excel_path = (
        Path(args.excel_path).expanduser().resolve()
        if args.excel_path
        else (base_dir / "config" / "config_incertezas_rev3.xlsx").resolve()
    )
    return launch_config_gui(base_dir=base_dir, config_dir=config_dir, excel_path=excel_path)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
