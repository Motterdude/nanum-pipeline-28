from __future__ import annotations

import argparse
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Optional, Sequence

try:
    import tkinter as tk
    from tkinter import filedialog, messagebox, scrolledtext, ttk
except Exception:
    tk = None
    filedialog = None
    messagebox = None
    scrolledtext = None
    ttk = None


DEFAULT_OPENTOCSV_CANDIDATES = [
    Path(r"C:\Program Files (x86)\Kistler\CSVExportSeriell\OpenToCSV.exe"),
    Path(r"C:\Program Files\Kistler\CSVExportSeriell\OpenToCSV.exe"),
]
DEFAULT_SETTINGS_DIR = Path(os.environ.get("LOCALAPPDATA", str(Path.home()))) / "nanum_pipeline_28"
SETTINGS_PATH = DEFAULT_SETTINGS_DIR / "kibox_open_to_csv_settings.json"
SUPPORTED_EXPORT_TYPES = {"res", "sig", "tim"}
SUPPORTED_NAME_MODES = {"source", "pipeline", "tool"}
PIPELINE_RESULT_SUFFIX = "_i.csv"
GUI_LOG_POLL_MS = 75
KW_TOKEN_RE = re.compile(r"\d+(?:[.,]\d+)?\s*[-_ ]?\s*kw", flags=re.IGNORECASE)
NAME_DELIMITERS = "_- "
GUI_DYNAMIC_INSERT_POSITION_KEYS = [
    "start",
    "after_first_underscore",
    "after_kw_token",
    "before_date_block",
    "end",
]

LogCallback = Callable[[str, bool], None]


@dataclass(frozen=True)
class ExportRequest:
    source_open: Path
    destination_dir: Path
    export_type: str = "res"
    separator: str = "tab"
    include_cycle_number: bool = True
    cycles: Optional[str] = None
    name_mode: str = "source"
    output_name: Optional[str] = None


@dataclass(frozen=True)
class ExportResult:
    source_open: Path
    exported_csv: Path
    tool_generated_csv: Path
    returncode: int
    stdout: str
    stderr: str


def load_gui_settings() -> dict:
    try:
        if not SETTINGS_PATH.exists():
            return {}
        return json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_gui_settings(settings: dict) -> None:
    DEFAULT_SETTINGS_DIR.mkdir(parents=True, exist_ok=True)
    SETTINGS_PATH.write_text(json.dumps(settings, indent=2, ensure_ascii=True), encoding="utf-8")


def remember_opentocsv_path(path: Path) -> None:
    settings = load_gui_settings()
    settings["opentocsv_path"] = str(path.expanduser().resolve())
    save_gui_settings(settings)


def _saved_opentocsv_path() -> Optional[Path]:
    settings = load_gui_settings()
    raw = str(settings.get("opentocsv_path", "")).strip()
    if not raw:
        return None
    try:
        return Path(raw).expanduser().resolve()
    except Exception:
        return None


def find_opentocsv_exe(explicit_path: Optional[Path] = None) -> Path:
    if explicit_path is not None:
        p = explicit_path.expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"OpenToCSV.exe nao encontrado em: {p}")
        remember_opentocsv_path(p)
        return p

    saved_candidate = _saved_opentocsv_path()
    candidates: list[Path] = []
    if saved_candidate is not None:
        candidates.append(saved_candidate)
    for candidate in DEFAULT_OPENTOCSV_CANDIDATES:
        if candidate not in candidates:
            candidates.append(candidate)

    for candidate in candidates:
        if candidate.exists():
            remember_opentocsv_path(candidate)
            return candidate

    raise FileNotFoundError(
        "Nao encontrei o OpenToCSV.exe. Procurei em: "
        + ", ".join(str(p) for p in candidates)
    )


def _normalize_export_type(export_type: str) -> str:
    value = str(export_type).strip().lower()
    if value not in SUPPORTED_EXPORT_TYPES:
        raise ValueError(f"export_type invalido: {export_type}. Use um de {sorted(SUPPORTED_EXPORT_TYPES)}.")
    return value


def _normalize_separator(separator: str) -> str:
    value = str(separator).strip().lower()
    if value in {"tab", "\\t", "t"}:
        return "tab"
    if value in {",", "comma"}:
        return ","
    if value in {";", "semicolon"}:
        return ";"
    raise ValueError("separator invalido. Use 'tab', ',' ou ';'.")


def _normalize_name_mode(name_mode: str) -> str:
    value = str(name_mode).strip().lower()
    if value not in SUPPORTED_NAME_MODES:
        raise ValueError(f"name_mode invalido: {name_mode}. Use um de {sorted(SUPPORTED_NAME_MODES)}.")
    return value


def _normalize_output_name(output_name: Optional[str]) -> Optional[str]:
    if output_name is None:
        return None
    value = str(output_name).strip()
    if not value:
        return None
    if not value.lower().endswith(".csv"):
        value += ".csv"
    return Path(value).name


def _build_converter_args(
    source_dir: Path,
    *,
    export_type: str,
    separator: str,
    include_cycle_number: bool,
    cycles: Optional[str],
) -> list[str]:
    args = [f"sourcepath={source_dir}", f"type={export_type}"]
    if separator == "tab":
        args.append("sep=tab")
    elif separator == ",":
        args.append("sep=,")
    elif separator == ";":
        pass
    else:
        raise ValueError(f"Separador nao suportado: {separator}")

    if include_cycle_number:
        args.append("cno")
    if cycles:
        args.append(f"cycles={cycles}")
    return args


def _default_output_name(source_open: Path, *, name_mode: str, export_type: str) -> str:
    if name_mode == "tool":
        return f"{source_open.stem}_{export_type}.csv"
    if name_mode == "pipeline":
        if export_type != "res":
            raise ValueError("name_mode='pipeline' exige export_type='res'.")
        return f"{source_open.stem}{PIPELINE_RESULT_SUFFIX}"
    return f"{source_open.stem}.csv"


def _default_output_stem(source_open: Path, *, name_mode: str, export_type: str) -> str:
    if name_mode == "tool":
        return f"{source_open.stem}_{export_type}"
    if name_mode == "pipeline":
        if export_type != "res":
            raise ValueError("name_mode='pipeline' exige export_type='res'.")
        return f"{source_open.stem}_i"
    return source_open.stem


def _resolve_insert_index(base_stem: str, *, position: str, anchor_text: str = "") -> int:
    if position == "start":
        return 0
    elif position == "before_first_underscore":
        first_underscore = base_stem.find("_")
        if first_underscore < 0:
            raise ValueError(f"Nao encontrei '_' em '{base_stem}'.")
        return first_underscore
    elif position == "after_first_underscore":
        first_underscore = base_stem.find("_")
        if first_underscore < 0:
            raise ValueError(f"Nao encontrei '_' em '{base_stem}'.")
        return first_underscore + 1
    elif position == "before_kw_token":
        match = KW_TOKEN_RE.search(base_stem)
        if match is None:
            raise ValueError(f"Nao encontrei token de carga tipo '17,5KW' em '{base_stem}'.")
        return match.start()
    elif position == "after_kw_token":
        match = KW_TOKEN_RE.search(base_stem)
        if match is None:
            raise ValueError(f"Nao encontrei token de carga tipo '17,5KW' em '{base_stem}'.")
        return match.end()
    elif position == "before_date_block":
        match = re.search(r"-\d{4}-\d{2}-\d{2}", base_stem)
        if match is None:
            raise ValueError(f"Nao encontrei bloco de data tipo '-2026-03-06' em '{base_stem}'.")
        return match.start()
    elif position == "before_i_suffix":
        if base_stem.endswith("_i"):
            return len(base_stem) - 2
        return len(base_stem)
    elif position == "end":
        return len(base_stem)
    elif position == "before_anchor":
        if not anchor_text:
            raise ValueError("O texto alvo da insercao esta vazio.")
        anchor_idx = base_stem.find(anchor_text)
        if anchor_idx < 0:
            raise ValueError(f"Nao encontrei o texto alvo '{anchor_text}' em '{base_stem}'.")
        return anchor_idx
    elif position == "after_anchor":
        if not anchor_text:
            raise ValueError("O texto alvo da insercao esta vazio.")
        anchor_idx = base_stem.find(anchor_text)
        if anchor_idx < 0:
            raise ValueError(f"Nao encontrei o texto alvo '{anchor_text}' em '{base_stem}'.")
        return anchor_idx + len(anchor_text)
    else:
        raise ValueError(f"Posicao de insercao nao suportada: {position}")

def _insert_text_at_position(base_stem: str, insert_text: str, *, position: str, anchor_text: str = "") -> str:
    if not insert_text or position == "none":
        return base_stem
    insert_idx = _resolve_insert_index(base_stem, position=position, anchor_text=anchor_text)
    payload = str(insert_text)
    prev_char = base_stem[insert_idx - 1] if insert_idx > 0 else ""
    next_char = base_stem[insert_idx] if insert_idx < len(base_stem) else ""

    if prev_char and prev_char not in NAME_DELIMITERS and payload[0] not in NAME_DELIMITERS:
        payload = "_" + payload
    if next_char and next_char not in NAME_DELIMITERS and payload[-1] not in NAME_DELIMITERS:
        payload = payload + "_"
    if next_char == "-" and payload[-1] not in NAME_DELIMITERS:
        payload = payload + "_"

    return f"{base_stem[:insert_idx]}{payload}{base_stem[insert_idx:]}"


def build_output_name(
    source_open: Path,
    *,
    name_mode: str,
    export_type: str,
    output_name: Optional[str] = None,
    insert_text: str = "",
    insert_position: str = "none",
    insert_anchor: str = "",
) -> str:
    normalized_output_name = _normalize_output_name(output_name)
    if normalized_output_name is not None:
        return normalized_output_name

    base_stem = _default_output_stem(source_open, name_mode=name_mode, export_type=export_type)
    final_stem = _insert_text_at_position(
        base_stem,
        str(insert_text or ""),
        position=str(insert_position or "none"),
        anchor_text=str(insert_anchor or ""),
    )
    return f"{final_stem}.csv"


def _find_tool_output(export_dir: Path, *, source_stem: str, export_type: str) -> Path:
    expected = export_dir / f"{source_stem}_{export_type}.csv"
    if expected.exists():
        return expected

    csvs = sorted(export_dir.glob("*.csv"))
    if len(csvs) == 1:
        return csvs[0]

    matches = [p for p in csvs if p.stem.lower().startswith(source_stem.lower())]
    if len(matches) == 1:
        return matches[0]

    raise FileNotFoundError(
        f"Nao encontrei o CSV exportado em {export_dir}. Esperava '{expected.name}' e achei: {[p.name for p in csvs]}"
    )


def _stream_pipe(stream, *, is_stderr: bool, chunks: list[str], log_callback: Optional[LogCallback]) -> None:
    if stream is None:
        return

    pending: list[str] = []
    while True:
        ch = stream.read(1)
        if ch == "":
            break
        chunks.append(ch)
        pending.append(ch)
        if ch == "\n" or len(pending) >= 64:
            if log_callback is not None:
                log_callback("".join(pending), is_stderr)
            pending.clear()

    if pending and log_callback is not None:
        log_callback("".join(pending), is_stderr)


def _run_converter(cmd: Sequence[str], *, cwd: Path, log_callback: Optional[LogCallback]) -> subprocess.CompletedProcess[str]:
    proc = subprocess.Popen(
        list(cmd),
        cwd=str(cwd),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []
    stdout_thread = threading.Thread(
        target=_stream_pipe,
        args=(proc.stdout,),
        kwargs={"is_stderr": False, "chunks": stdout_chunks, "log_callback": log_callback},
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_stream_pipe,
        args=(proc.stderr,),
        kwargs={"is_stderr": True, "chunks": stderr_chunks, "log_callback": log_callback},
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()
    returncode = proc.wait()
    stdout_thread.join()
    stderr_thread.join()
    return subprocess.CompletedProcess(
        args=list(cmd),
        returncode=returncode,
        stdout="".join(stdout_chunks),
        stderr="".join(stderr_chunks),
    )


def export_open_file(
    request: ExportRequest,
    *,
    converter_path: Optional[Path] = None,
    log_callback: Optional[LogCallback] = None,
) -> ExportResult:
    source_open = request.source_open.expanduser().resolve()
    if not source_open.exists():
        raise FileNotFoundError(f"Arquivo .open nao encontrado: {source_open}")
    if source_open.suffix.lower() != ".open":
        raise ValueError(f"Arquivo de entrada nao e .open: {source_open}")

    export_type = _normalize_export_type(request.export_type)
    separator = _normalize_separator(request.separator)
    name_mode = _normalize_name_mode(request.name_mode)
    output_name = _normalize_output_name(request.output_name)
    dest_dir = request.destination_dir.expanduser().resolve()
    dest_dir.mkdir(parents=True, exist_ok=True)

    exe = find_opentocsv_exe(converter_path)
    with tempfile.TemporaryDirectory(prefix="kibox_open_to_csv_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        source_dir = temp_dir / "source"
        source_dir.mkdir(parents=True, exist_ok=True)
        temp_open = source_dir / source_open.name
        shutil.copy2(source_open, temp_open)

        cmd = [
            str(exe),
            *_build_converter_args(
                source_dir,
                export_type=export_type,
                separator=separator,
                include_cycle_number=request.include_cycle_number,
                cycles=request.cycles,
            ),
        ]
        proc = _run_converter(cmd, cwd=exe.parent, log_callback=log_callback)

        export_dir = source_dir / "CSVExport"
        tool_generated_csv = _find_tool_output(export_dir, source_stem=source_open.stem, export_type=export_type)
        final_name = build_output_name(
            source_open,
            name_mode=name_mode,
            export_type=export_type,
            output_name=output_name,
        )
        final_path = dest_dir / final_name
        shutil.copy2(tool_generated_csv, final_path)

        return ExportResult(
            source_open=source_open,
            exported_csv=final_path,
            tool_generated_csv=tool_generated_csv,
            returncode=proc.returncode,
            stdout=proc.stdout,
            stderr=proc.stderr,
        )


def _iter_open_files(input_path: Path) -> Iterable[Path]:
    if input_path.is_file():
        yield input_path
        return

    for path in sorted(input_path.rglob("*.open")):
        if path.is_file():
            yield path


def _destination_dir_for(source_open: Path, *, input_root: Path, output_root: Optional[Path]) -> Path:
    if output_root is None:
        return source_open.parent
    if input_root.is_file():
        return output_root
    rel_parent = source_open.parent.relative_to(input_root)
    return output_root / rel_parent


def export_open_inputs(
    input_path: Path,
    *,
    output_root: Optional[Path] = None,
    converter_path: Optional[Path] = None,
    export_type: str = "res",
    separator: str = "tab",
    include_cycle_number: bool = True,
    cycles: Optional[str] = None,
    name_mode: str = "source",
    output_name: Optional[str] = None,
    log_callback: Optional[LogCallback] = None,
) -> list[ExportResult]:
    input_path = input_path.expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Entrada nao encontrada: {input_path}")
    if input_path.is_file() and input_path.suffix.lower() != ".open":
        raise ValueError(f"Entrada de arquivo precisa ser .open: {input_path}")
    if input_path.is_dir() and output_name:
        raise ValueError("output_name so pode ser usado quando a entrada for um unico arquivo .open.")

    files = list(_iter_open_files(input_path))
    if not files:
        raise FileNotFoundError(f"Nao encontrei arquivos .open em: {input_path}")

    resolved_output_root = None if output_root is None else output_root.expanduser().resolve()
    results: list[ExportResult] = []
    for source_open in files:
        dest_dir = _destination_dir_for(source_open, input_root=input_path, output_root=resolved_output_root)
        result = export_open_file(
            ExportRequest(
                source_open=source_open,
                destination_dir=dest_dir,
                export_type=export_type,
                separator=separator,
                include_cycle_number=include_cycle_number,
                cycles=cycles,
                name_mode=name_mode,
                output_name=output_name,
            ),
            converter_path=converter_path,
            log_callback=log_callback,
        )
        results.append(result)
    return results


def _planned_pipeline_csv_name(source_open: Path) -> str:
    return build_output_name(source_open, name_mode="pipeline", export_type="res")


def _find_duplicate_planned_outputs(
    source_files: Sequence[Path],
    destination_dir: Path,
    *,
    naming_fn: Callable[[Path], str],
) -> dict[str, list[Path]]:
    collisions: dict[str, list[Path]] = {}
    for source in source_files:
        planned = str((destination_dir / naming_fn(source)).resolve()).lower()
        collisions.setdefault(planned, []).append(source)
    return {k: v for k, v in collisions.items() if len(v) > 1}


class OpenToCsvGuiApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("KiBox .open -> .csv")
        self.root.geometry("1080x760")
        self.root.minsize(900, 640)

        self.selected_files: list[Path] = []
        self.event_queue: queue.Queue[tuple] = queue.Queue()
        self.worker_thread: Optional[threading.Thread] = None

        self.output_dir_var = tk.StringVar(master=self.root, value="")
        self.converter_var = tk.StringVar(master=self.root, value="")
        self.insert_text_var = tk.StringVar(master=self.root, value="")
        self.insert_position_var = tk.StringVar(master=self.root, value="")
        self.status_var = tk.StringVar(master=self.root, value="Selecione os arquivos .open e o diretorio de saida.")
        self.current_file_var = tk.StringVar(master=self.root, value="Nenhuma conversao em andamento.")
        self.count_var = tk.StringVar(master=self.root, value="0 arquivo(s) selecionado(s)")
        self.preview_var = tk.StringVar(master=self.root, value="Previa: nenhum arquivo selecionado.")
        self.insert_option_label_to_key: dict[str, str] = {}

        try:
            self.converter_var.set(str(find_opentocsv_exe()))
        except Exception:
            self.converter_var.set("")

        self.insert_text_var.trace_add("write", self._on_naming_var_changed)

        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(50, self._bootstrap_converter_path)

    def _build_ui(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(3, weight=1)

        selection_frame = ttk.LabelFrame(self.root, text="Arquivos .open")
        selection_frame.grid(row=0, column=0, sticky="nsew", padx=10, pady=(10, 6))
        selection_frame.columnconfigure(0, weight=1)
        selection_frame.rowconfigure(1, weight=1)

        button_row = ttk.Frame(selection_frame)
        button_row.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 4))
        self.add_files_button = ttk.Button(button_row, text="Adicionar arquivos", command=self._add_files)
        self.add_files_button.pack(side="left")
        self.remove_files_button = ttk.Button(button_row, text="Remover selecionados", command=self._remove_selected_files)
        self.remove_files_button.pack(side="left", padx=(8, 0))
        self.clear_files_button = ttk.Button(button_row, text="Limpar lista", command=self._clear_files)
        self.clear_files_button.pack(side="left", padx=(8, 0))
        ttk.Label(button_row, textvariable=self.count_var).pack(side="right")

        list_frame = ttk.Frame(selection_frame)
        list_frame.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)

        self.file_listbox = tk.Listbox(list_frame, selectmode=tk.EXTENDED, width=140, height=12)
        self.file_listbox.grid(row=0, column=0, sticky="nsew")
        file_scroll = ttk.Scrollbar(list_frame, orient="vertical", command=self.file_listbox.yview)
        file_scroll.grid(row=0, column=1, sticky="ns")
        self.file_listbox.configure(yscrollcommand=file_scroll.set)

        config_frame = ttk.LabelFrame(self.root, text="Destino")
        config_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=6)
        config_frame.columnconfigure(1, weight=1)

        ttk.Label(config_frame, text="Diretorio de saida").grid(row=0, column=0, sticky="w", padx=8, pady=8)
        self.output_dir_entry = ttk.Entry(config_frame, textvariable=self.output_dir_var)
        self.output_dir_entry.grid(row=0, column=1, sticky="ew", padx=(0, 8), pady=8)
        self.output_dir_button = ttk.Button(config_frame, text="Selecionar pasta", command=self._select_output_dir)
        self.output_dir_button.grid(row=0, column=2, sticky="e", padx=(0, 8), pady=8)

        ttk.Label(config_frame, text="OpenToCSV.exe").grid(row=1, column=0, sticky="w", padx=8, pady=(0, 8))
        self.converter_entry = ttk.Entry(config_frame, textvariable=self.converter_var)
        self.converter_entry.grid(row=1, column=1, sticky="ew", padx=(0, 8), pady=(0, 8))
        self.converter_button = ttk.Button(config_frame, text="Selecionar exe", command=self._select_converter)
        self.converter_button.grid(row=1, column=2, sticky="e", padx=(0, 8), pady=(0, 8))

        ttk.Label(config_frame, text="Texto para inserir").grid(row=2, column=0, sticky="w", padx=8, pady=(0, 8))
        self.insert_text_entry = ttk.Entry(config_frame, textvariable=self.insert_text_var)
        self.insert_text_entry.grid(row=2, column=1, sticky="ew", padx=(0, 8), pady=(0, 8))
        ttk.Label(config_frame, text="Ex.: aditivado_subindo_1_").grid(row=2, column=2, sticky="w", padx=(0, 8), pady=(0, 8))

        ttk.Label(config_frame, text="Posicao de insercao").grid(row=3, column=0, sticky="w", padx=8, pady=(0, 8))
        self.insert_position_combo = ttk.Combobox(
            config_frame,
            textvariable=self.insert_position_var,
            state="readonly",
            values=[],
        )
        self.insert_position_combo.grid(row=3, column=1, sticky="ew", padx=(0, 8), pady=(0, 8))
        ttk.Label(
            config_frame,
            text="O dropdown usa o nome do arquivo selecionado na lista como amostra visual.",
        ).grid(row=3, column=2, sticky="w", padx=(0, 8), pady=(0, 8))

        ttk.Label(config_frame, text="Previa do nome").grid(row=4, column=0, sticky="nw", padx=8, pady=(0, 8))
        self.preview_label = ttk.Label(config_frame, textvariable=self.preview_var, wraplength=780, justify="left")
        self.preview_label.grid(row=4, column=1, columnspan=2, sticky="w", padx=(0, 8), pady=(0, 8))

        progress_frame = ttk.LabelFrame(self.root, text="Andamento")
        progress_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=6)
        progress_frame.columnconfigure(0, weight=1)

        ttk.Label(progress_frame, textvariable=self.current_file_var).grid(row=0, column=0, sticky="w", padx=8, pady=(8, 4))
        self.current_progress = ttk.Progressbar(progress_frame, mode="indeterminate")
        self.current_progress.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 6))
        self.overall_progress = ttk.Progressbar(progress_frame, mode="determinate", maximum=1, value=0)
        self.overall_progress.grid(row=2, column=0, sticky="ew", padx=8, pady=(0, 8))

        log_frame = ttk.LabelFrame(self.root, text="Log")
        log_frame.grid(row=3, column=0, sticky="nsew", padx=10, pady=6)
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        self.log_widget = scrolledtext.ScrolledText(log_frame, wrap="word", height=18)
        self.log_widget.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
        self.log_widget.configure(state="disabled")

        action_frame = ttk.Frame(self.root)
        action_frame.grid(row=4, column=0, sticky="ew", padx=10, pady=(4, 10))
        action_frame.columnconfigure(0, weight=1)
        ttk.Label(action_frame, textvariable=self.status_var).grid(row=0, column=0, sticky="w")
        self.start_button = ttk.Button(action_frame, text="Converter arquivos", command=self._start_conversion)
        self.start_button.grid(row=0, column=1, sticky="e")
        self.file_listbox.bind("<<ListboxSelect>>", self._on_file_selection_changed)
        self.insert_position_combo.bind("<<ComboboxSelected>>", self._on_insert_position_selected)
        self._refresh_insert_options()
        self._update_preview()

    def _set_converter_path(self, raw_path: str, *, show_dialog_on_error: bool = True) -> bool:
        value = str(raw_path).strip()
        if not value:
            self.converter_var.set("")
            return False

        try:
            resolved = find_opentocsv_exe(Path(value))
        except Exception as exc:
            self.converter_var.set("")
            self.status_var.set("OpenToCSV.exe ainda nao configurado.")
            if show_dialog_on_error:
                messagebox.showerror("OpenToCSV", str(exc))
            return False

        self.converter_var.set(str(resolved))
        self.status_var.set(f"OpenToCSV configurado: {resolved}")
        return True

    def _bootstrap_converter_path(self) -> None:
        current = self.converter_var.get().strip()
        if current:
            return

        saved_path = _saved_opentocsv_path()
        if saved_path is not None:
            prompt = (
                "O caminho salvo do OpenToCSV.exe nao existe neste computador:\n"
                f"{saved_path}\n\n"
                "Selecione o OpenToCSV.exe para salvar o novo local."
            )
        else:
            prompt = (
                "Nao encontrei o OpenToCSV.exe automaticamente nesta primeira abertura.\n\n"
                "Selecione o executavel uma vez para salvar o caminho neste computador."
            )

        self.status_var.set("Selecione o OpenToCSV.exe para concluir a configuracao inicial.")
        wants_to_select = messagebox.askyesno("Configurar OpenToCSV", prompt)
        if not wants_to_select:
            self.status_var.set("Conversao desabilitada ate configurar o OpenToCSV.exe.")
            return
        self._select_converter()

    def _set_controls_enabled(self, enabled: bool) -> None:
        state = "normal" if enabled else "disabled"
        widgets = [
            self.add_files_button,
            self.remove_files_button,
            self.clear_files_button,
            self.output_dir_entry,
            self.output_dir_button,
            self.converter_entry,
            self.converter_button,
            self.insert_text_entry,
            self.insert_position_combo,
            self.start_button,
        ]
        for widget in widgets:
            widget.configure(state=state)

    def _selected_insert_position_key(self) -> str:
        label = self.insert_position_var.get().strip()
        return self.insert_option_label_to_key.get(label, "after_first_underscore")

    def _preview_source_file(self) -> Optional[Path]:
        selected_indexes = list(self.file_listbox.curselection())
        if selected_indexes:
            idx = selected_indexes[0]
            if 0 <= idx < len(self.selected_files):
                return self.selected_files[idx]
        return self.selected_files[0] if self.selected_files else None

    def _render_insert_choice_label(self, source_open: Path, position_key: str) -> str:
        marker = self.insert_text_var.get() or "xxxx_"
        source_stem = source_open.stem
        rendered = _insert_text_at_position(source_stem, marker, position=position_key)
        return f"{rendered}.open"

    def _available_insert_options(self, source_open: Path) -> list[tuple[str, str]]:
        options: list[tuple[str, str]] = []
        seen_labels: set[str] = set()
        for position_key in GUI_DYNAMIC_INSERT_POSITION_KEYS:
            try:
                label = self._render_insert_choice_label(source_open, position_key)
            except Exception:
                continue
            if label in seen_labels:
                continue
            seen_labels.add(label)
            options.append((position_key, label))
        return options

    def _refresh_insert_options(self, preferred_key: Optional[str] = None) -> None:
        source = self._preview_source_file()
        if source is None:
            self.insert_option_label_to_key = {}
            self.insert_position_combo.configure(values=[])
            self.insert_position_var.set("")
            return

        options = self._available_insert_options(source)
        self.insert_option_label_to_key = {label: key for key, label in options}
        labels = [label for _, label in options]
        self.insert_position_combo.configure(values=labels)
        if not labels:
            self.insert_position_var.set("")
            return

        current_key = preferred_key or self._selected_insert_position_key()
        selected_label = None
        for key, label in options:
            if key == current_key:
                selected_label = label
                break
        if selected_label is None:
            for key, label in options:
                if key == "after_first_underscore":
                    selected_label = label
                    break
        if selected_label is None:
            selected_label = labels[0]
        self.insert_position_var.set(selected_label)

    def _planned_gui_output_name(self, source_open: Path) -> str:
        return build_output_name(
            source_open,
            name_mode="pipeline",
            export_type="res",
            insert_text=self.insert_text_var.get(),
            insert_position=self._selected_insert_position_key(),
        )

    def _update_preview(self) -> None:
        source = self._preview_source_file()
        if source is None:
            self.preview_var.set("Previa: nenhum arquivo selecionado.")
            return
        try:
            planned_name = self._planned_gui_output_name(source)
            self.preview_var.set(f"Previa: {source.name} -> {planned_name}")
        except Exception as exc:
            self.preview_var.set(f"Previa: configuracao invalida ({exc})")

    def _on_naming_var_changed(self, *_args) -> None:
        self._refresh_insert_options()
        self._update_preview()

    def _on_file_selection_changed(self, _event=None) -> None:
        self._refresh_insert_options()
        self._update_preview()

    def _on_insert_position_selected(self, _event=None) -> None:
        self._update_preview()

    def _append_log(self, text: str) -> None:
        if not text:
            return
        self.log_widget.configure(state="normal")
        self.log_widget.insert("end", text)
        self.log_widget.see("end")
        self.log_widget.configure(state="disabled")

    def _refresh_file_list(self) -> None:
        self.file_listbox.delete(0, "end")
        for path in self.selected_files:
            self.file_listbox.insert("end", str(path))
        self.count_var.set(f"{len(self.selected_files)} arquivo(s) selecionado(s)")
        self._update_preview()

    def _add_files(self) -> None:
        selected = filedialog.askopenfilenames(
            title="Selecione os arquivos .open",
            filetypes=[("KiBox open files", "*.open"), ("Todos os arquivos", "*.*")],
        )
        if not selected:
            return

        seen = {str(p).lower() for p in self.selected_files}
        for raw_path in selected:
            path = Path(raw_path).expanduser().resolve()
            if path.suffix.lower() != ".open":
                continue
            key = str(path).lower()
            if key in seen:
                continue
            self.selected_files.append(path)
            seen.add(key)

        self.selected_files.sort(key=lambda p: str(p).lower())
        self._refresh_file_list()

    def _remove_selected_files(self) -> None:
        indexes = list(self.file_listbox.curselection())
        if not indexes:
            return
        index_set = set(indexes)
        self.selected_files = [p for idx, p in enumerate(self.selected_files) if idx not in index_set]
        self._refresh_file_list()

    def _clear_files(self) -> None:
        self.selected_files.clear()
        self._refresh_file_list()

    def _select_output_dir(self) -> None:
        selected = filedialog.askdirectory(title="Selecione o diretorio de saida")
        if selected:
            self.output_dir_var.set(selected)

    def _select_converter(self) -> None:
        initialdir = None
        current = self.converter_var.get().strip()
        if current:
            try:
                initialdir = str(Path(current).expanduser().resolve().parent)
            except Exception:
                initialdir = None
        if not initialdir:
            saved_path = _saved_opentocsv_path()
            if saved_path is not None:
                initialdir = str(saved_path.parent)
        if not initialdir:
            for candidate in DEFAULT_OPENTOCSV_CANDIDATES:
                if candidate.parent.exists():
                    initialdir = str(candidate.parent)
                    break

        selected = filedialog.askopenfilename(
            title="Selecione o OpenToCSV.exe",
            initialdir=initialdir,
            filetypes=[("Executavel", "*.exe"), ("Todos os arquivos", "*.*")],
        )
        if selected:
            self._set_converter_path(selected)

    def _start_conversion(self) -> None:
        if self.worker_thread is not None and self.worker_thread.is_alive():
            return

        if not self.selected_files:
            messagebox.showwarning("Conversao", "Selecione pelo menos um arquivo .open.")
            return

        output_dir_raw = self.output_dir_var.get().strip()
        if not output_dir_raw:
            messagebox.showwarning("Conversao", "Selecione o diretorio de saida.")
            return

        output_dir = Path(output_dir_raw).expanduser().resolve()
        converter_raw = self.converter_var.get().strip()
        converter_path = Path(converter_raw).expanduser().resolve() if converter_raw else None

        try:
            output_dir.mkdir(parents=True, exist_ok=True)
            resolved_converter = find_opentocsv_exe(converter_path)
        except Exception as exc:
            messagebox.showerror("Conversao", str(exc))
            return
        self.converter_var.set(str(resolved_converter))

        try:
            planned_names = {source: self._planned_gui_output_name(source) for source in self.selected_files}
        except Exception as exc:
            messagebox.showerror("Conversao", f"Configuracao de nome invalida:\n{exc}")
            return

        duplicates = _find_duplicate_planned_outputs(
            self.selected_files,
            output_dir,
            naming_fn=lambda source: planned_names[source],
        )
        if duplicates:
            lines = ["Mais de um arquivo geraria o mesmo nome final no diretorio de saida:"]
            for target, sources in sorted(duplicates.items()):
                lines.append(target)
                for source in sources:
                    lines.append(f"  - {source}")
            messagebox.showerror("Conversao", "\n".join(lines))
            return

        existing = sorted(
            str((output_dir / planned_names[source]).resolve())
            for source in self.selected_files
            if (output_dir / planned_names[source]).exists()
        )
        if existing:
            proceed = messagebox.askyesno(
                "Sobrescrever arquivos",
                "Alguns CSVs de destino ja existem e serao sobrescritos.\n\n"
                + "\n".join(existing[:12])
                + ("\n..." if len(existing) > 12 else ""),
            )
            if not proceed:
                return

        self.log_widget.configure(state="normal")
        self.log_widget.delete("1.0", "end")
        self.log_widget.configure(state="disabled")
        self.status_var.set("Conversao em andamento...")
        self.current_file_var.set("Preparando conversao...")
        self.overall_progress.configure(maximum=len(self.selected_files), value=0)
        self.current_progress.start(12)
        self._set_controls_enabled(False)

        files = list(self.selected_files)
        self.worker_thread = threading.Thread(
            target=self._worker_convert,
            args=(files, output_dir, converter_path, planned_names),
            daemon=True,
        )
        self.worker_thread.start()
        self.root.after(GUI_LOG_POLL_MS, self._drain_events)

    def _worker_convert(
        self,
        files: Sequence[Path],
        output_dir: Path,
        converter_path: Optional[Path],
        planned_names: dict[Path, str],
    ) -> None:
        total = len(files)
        ok_count = 0
        fail_count = 0
        for idx, source_open in enumerate(files, start=1):
            self.event_queue.put(("start", idx, total, source_open))
            try:
                result = export_open_file(
                    ExportRequest(
                        source_open=source_open,
                        destination_dir=output_dir,
                        export_type="res",
                        separator="tab",
                        include_cycle_number=True,
                        name_mode="pipeline",
                        output_name=planned_names[source_open],
                    ),
                    converter_path=converter_path,
                    log_callback=lambda text, is_stderr, src=source_open: self.event_queue.put(("log", src, text, is_stderr)),
                )
                self.event_queue.put(("done", idx, total, result))
                ok_count += 1
            except Exception as exc:
                self.event_queue.put(("error", idx, total, source_open, str(exc)))
                fail_count += 1
        self.event_queue.put(("finished", total, ok_count, fail_count))

    def _drain_events(self) -> None:
        while True:
            try:
                event = self.event_queue.get_nowait()
            except queue.Empty:
                break
            self._handle_event(event)

        still_running = self.worker_thread is not None and self.worker_thread.is_alive()
        if still_running or not self.event_queue.empty():
            self.root.after(GUI_LOG_POLL_MS, self._drain_events)

    def _handle_event(self, event: tuple) -> None:
        event_type = event[0]
        if event_type == "start":
            _, idx, total, source_open = event
            self.current_file_var.set(f"Convertendo {idx}/{total}: {source_open.name}")
            self._append_log(f"\n=== {source_open.name} ===\n")
            return

        if event_type == "log":
            _, source_open, text, is_stderr = event
            prefix = "[stderr] " if is_stderr else ""
            self._append_log(prefix + text if prefix and not text.startswith(prefix) else text)
            return

        if event_type == "done":
            _, idx, total, result = event
            self.overall_progress.configure(value=idx)
            self._append_log(f"\n[OK] {result.source_open.name} -> {result.exported_csv.name}\n")
            if idx == total:
                self.current_file_var.set("Finalizando conversao...")
            return

        if event_type == "error":
            _, idx, total, source_open, error_text = event
            self.overall_progress.configure(value=idx)
            self._append_log(f"\n[ERROR] {source_open.name}: {error_text}\n")
            if idx == total:
                self.current_file_var.set("Finalizando conversao...")
            return

        if event_type == "finished":
            _, total, ok_count, fail_count = event
            self.current_progress.stop()
            self.current_file_var.set("Conversao concluida.")
            self.status_var.set(f"Concluido: {ok_count} OK, {fail_count} erro(s), total {total}.")
            self._set_controls_enabled(True)
            return

    def _on_close(self) -> None:
        if self.worker_thread is not None and self.worker_thread.is_alive():
            close_anyway = messagebox.askyesno(
                "Conversao em andamento",
                "A conversao ainda esta em andamento. Fechar a janela nao interrompe o OpenToCSV com seguranca.\n\nDeseja fechar mesmo assim?",
            )
            if not close_anyway:
                return
        self.root.destroy()


def launch_gui() -> int:
    if tk is None or ttk is None or filedialog is None or scrolledtext is None:
        print("[ERROR] Tkinter nao esta disponivel neste Python. A GUI nao pode ser aberta.", file=sys.stderr)
        return 1

    root = tk.Tk()
    app = OpenToCsvGuiApp(root)
    _ = app
    root.mainloop()
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Converte arquivos .open do KiBox para CSV usando o utilitario OpenToCSV instalado com o KiBox Cockpit."
    )
    parser.add_argument("input", nargs="?", type=Path, help="Arquivo .open unico ou diretorio contendo arquivos .open.")
    parser.add_argument("--output-dir", type=Path, default=None, help="Diretorio de saida. Para diretorio de entrada, preserva subpastas relativas.")
    parser.add_argument("--converter", type=Path, default=None, help="Caminho explicito para OpenToCSV.exe.")
    parser.add_argument("--type", dest="export_type", choices=sorted(SUPPORTED_EXPORT_TYPES), default="res", help="Tipo exportado pelo OpenToCSV. Default: res.")
    parser.add_argument("--separator", choices=["tab", ",", ";"], default="tab", help="Separador do CSV final. Default: tab.")
    parser.add_argument("--cycles", default=None, help="Faixa de ciclos no formato n-m. O padrao exporta todos os ciclos.")
    parser.add_argument("--no-cycle-number", action="store_true", help="Nao cria a primeira coluna com numero do ciclo.")
    parser.add_argument("--name-mode", choices=sorted(SUPPORTED_NAME_MODES), default="source", help="source=mesmo stem do .open; pipeline=stem + _i.csv; tool=mantem sufixo _res/_sig/_tim.")
    parser.add_argument("--output-name", default=None, help="Nome final explicito do CSV. So vale para entrada de um unico arquivo.")
    parser.add_argument("--gui", action="store_true", help="Abre a interface grafica para selecionar varios arquivos e acompanhar a conversao em tempo real.")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.gui or args.input is None:
        return launch_gui()

    try:
        results = export_open_inputs(
            args.input,
            output_root=args.output_dir,
            converter_path=args.converter,
            export_type=args.export_type,
            separator=args.separator,
            include_cycle_number=not args.no_cycle_number,
            cycles=args.cycles,
            name_mode=args.name_mode,
            output_name=args.output_name,
            log_callback=lambda text, is_stderr: print(text, end="", file=sys.stderr if is_stderr else sys.stdout),
        )
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1

    for result in results:
        print(f"[OK] {result.source_open} -> {result.exported_csv}")
        if result.returncode != 0:
            print(f"[WARN] OpenToCSV retornou codigo {result.returncode} para {result.source_open.name}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
