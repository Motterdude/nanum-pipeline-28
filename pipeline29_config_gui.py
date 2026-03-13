from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QStatusBar,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
    QHeaderView,
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


class EditableTableSection(QWidget):
    def __init__(self, title: str, columns: List[str], parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.title = title
        self.columns = columns

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        toolbar = QHBoxLayout()
        toolbar.addWidget(QLabel(title))
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
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.ExtendedSelection)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)
        header.setStretchLastSection(True)
        layout.addWidget(self.table, 1)

        self.btn_add.clicked.connect(lambda: self.add_row())
        self.btn_duplicate.clicked.connect(self.duplicate_selected_rows)
        self.btn_remove.clicked.connect(self.remove_selected_rows)

    def add_row(self, values: Optional[Dict[str, Any]] = None) -> None:
        row = self.table.rowCount()
        self.table.insertRow(row)
        values = values or {}
        for col_idx, column in enumerate(self.columns):
            text = "" if values.get(column) is None else str(values.get(column))
            self.table.setItem(row, col_idx, QTableWidgetItem(text))

    def load_records(self, records: List[Dict[str, Any]]) -> None:
        self.table.setRowCount(0)
        for record in records:
            self.add_row(record)

    def remove_selected_rows(self) -> None:
        rows = sorted({item.row() for item in self.table.selectedItems()}, reverse=True)
        for row in rows:
            self.table.removeRow(row)

    def duplicate_selected_rows(self) -> None:
        rows = sorted({item.row() for item in self.table.selectedItems()})
        for row in rows:
            self.add_row(self.record_at(row))

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


class Pipeline29ConfigEditor(QMainWindow):
    def __init__(self, *, base_dir: Path, config_dir: Path, excel_path: Path) -> None:
        super().__init__()
        self.base_dir = base_dir
        self.config_dir = config_dir
        self.excel_path = excel_path
        self.last_preset_path = ""

        state = load_gui_state(default_gui_state_path())
        state_config_dir = state.get("config_dir", "")
        state_excel_path = state.get("excel_path", "")
        state_last_preset = state.get("last_preset_path", "")
        if not str(config_dir).strip() and state_config_dir:
            self.config_dir = Path(state_config_dir)
        if not str(excel_path).strip() and state_excel_path:
            self.excel_path = Path(state_excel_path)
        self.last_preset_path = state_last_preset

        self.setWindowTitle("Pipeline 29 Config Editor")
        self.resize(1600, 920)

        central = QWidget(self)
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(10)

        path_row = QHBoxLayout()
        path_row.addWidget(QLabel("Text config dir"))
        self.config_dir_edit = QLineEdit(str(self.config_dir))
        path_row.addWidget(self.config_dir_edit, 1)
        self.btn_browse_config_dir = QPushButton("Browse")
        path_row.addWidget(self.btn_browse_config_dir)
        path_row.addSpacing(12)
        path_row.addWidget(QLabel("Excel rev3"))
        self.excel_path_edit = QLineEdit(str(self.excel_path))
        path_row.addWidget(self.excel_path_edit, 1)
        self.btn_browse_excel = QPushButton("Browse")
        path_row.addWidget(self.btn_browse_excel)
        root.addLayout(path_row)

        actions = QHBoxLayout()
        self.btn_reload_text = QPushButton("Reload text")
        self.btn_import_excel = QPushButton("Import Excel -> text")
        self.btn_save_text = QPushButton("Save text config")
        self.btn_validate = QPushButton("Validate")
        self.btn_save_preset = QPushButton("Save preset")
        self.btn_load_preset = QPushButton("Load preset")
        actions.addWidget(self.btn_reload_text)
        actions.addWidget(self.btn_import_excel)
        actions.addWidget(self.btn_save_text)
        actions.addWidget(self.btn_validate)
        actions.addWidget(self.btn_save_preset)
        actions.addWidget(self.btn_load_preset)
        actions.addStretch(1)
        root.addLayout(actions)

        self.tabs = QTabWidget(self)
        root.addWidget(self.tabs, 1)

        self.defaults_table = EditableTableSection("Defaults", ["param", "value"])
        self.data_quality_table = EditableTableSection("Data Quality", ["param", "value"])
        self.mappings_table = EditableTableSection("Mappings", DEFAULT_MAPPING_COLUMNS)
        self.instruments_table = EditableTableSection("Instruments", DEFAULT_INSTRUMENT_COLUMNS)
        self.reporting_table = EditableTableSection("Reporting_Rounding", DEFAULT_REPORTING_COLUMNS)
        self.plots_table = EditableTableSection("Plots", DEFAULT_PLOT_COLUMNS)

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
        self.btn_reload_text.clicked.connect(self.reload_text_bundle)
        self.btn_import_excel.clicked.connect(self.import_from_excel)
        self.btn_save_text.clicked.connect(self.save_text_bundle)
        self.btn_validate.clicked.connect(self.validate_current_bundle)
        self.btn_save_preset.clicked.connect(self.save_preset)
        self.btn_load_preset.clicked.connect(self.load_preset)

        self._load_initial_bundle()

    def _current_config_dir(self) -> Path:
        raw = self.config_dir_edit.text().strip()
        if raw:
            return Path(raw).expanduser().resolve()
        return default_text_config_dir(self.base_dir)

    def _current_excel_path(self) -> Path:
        raw = self.excel_path_edit.text().strip()
        if raw:
            return Path(raw).expanduser().resolve()
        return self.base_dir / "config" / "config_incertezas_rev3.xlsx"

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
        mappings_records = []
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
            if not key:
                continue
            if not value_txt:
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
        self._load_bundle(saved)
        self._show_status(f"Saved text config to {config_dir}")

    def save_preset(self) -> None:
        bundle, errors = self._bundle_from_ui()
        if errors:
            QMessageBox.warning(self, "Pipeline 29", "\n".join(errors))
            return
        preset_dir = default_preset_dir()
        default_name = "pipeline29_preset.json"
        if self.last_preset_path:
            default_name = Path(self.last_preset_path).name
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
    window.show()
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
