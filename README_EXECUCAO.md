# Execucao e Ambiente

## O que precisa estar no Git
- Codigo principal:
  - `nanum_pipeline_28.py`
  - `standalone_kibox_cycle_plots.py`
  - `standalone_kibox_cycle_viewer.py`
  - `standalone_kibox_cycle_viewer_fast.py`
- Config obrigatoria:
  - `config/config_incertezas_rev3.xlsx`
  - `config/lhv.csv`
  - `config/rules_consumo.csv`
- Ambiente:
  - `requirements_pipeline.txt`
  - `requirements_gui_viewer.txt`
  - `requirements_full.txt`
  - `setup_env.ps1`
- Registro de mudancas:
  - `HANDOFF_GLOBAL.md`

## O que nao vai para o Git
- `.venv/`
- `raw/`
- `out/`
- `out_validation/`
- `__pycache__/`
- arquivos `*.open`

Esses itens ficam fora do versionamento para evitar conflito, excesso de tamanho e lixo de execucao.

## Estrutura esperada
```text
Processamentos/
|-- config/
|   |-- config_incertezas_rev3.xlsx
|   |-- lhv.csv
|   |-- rules_consumo.csv
|-- raw/
|-- out/
|-- out_validation/
|-- nanum_pipeline_28.py
|-- standalone_kibox_cycle_plots.py
|-- standalone_kibox_cycle_viewer.py
|-- standalone_kibox_cycle_viewer_fast.py
|-- requirements_pipeline.txt
|-- requirements_gui_viewer.txt
|-- requirements_full.txt
|-- setup_env.ps1
|-- HANDOFF_GLOBAL.md
```

## Criacao do ambiente
Pipeline apenas:
```powershell
powershell -ExecutionPolicy Bypass -File .\setup_env.ps1
```

Pipeline + viewer Qt rapido:
```powershell
powershell -ExecutionPolicy Bypass -File .\setup_env.ps1 -WithGui
```

Se quiser forcar um Python especifico:
```powershell
powershell -ExecutionPolicy Bypass -File .\setup_env.ps1 -WithGui -PythonExe "C:\Users\Rafael\AppData\Local\Python\pythoncore-3.14-64\python.exe"
```

## Como rodar o pipeline
1. Ajuste os caminhos de entrada e saida na aba `Defaults` de `config/config_incertezas_rev3.xlsx`.
2. Campos usados:
   - `RAW_INPUT_DIR`
   - `OUT_DIR`
3. Se esses campos ficarem vazios, o pipeline usa:
   - entrada: `raw/PROCESSAR`
   - saida: `out/`

Rodar:
```powershell
& ".\.venv\Scripts\python.exe" .\nanum_pipeline_28.py
```

## Como rodar os utilitarios KIBOX
Plot estatico por blocos:
```powershell
& ".\.venv\Scripts\python.exe" .\standalone_kibox_cycle_plots.py
```

Viewer matplotlib:
```powershell
& ".\.venv\Scripts\python.exe" .\standalone_kibox_cycle_viewer.py
```

Viewer rapido Qt:
```powershell
& ".\.venv\Scripts\python.exe" .\standalone_kibox_cycle_viewer_fast.py
```

## Regras para evitar problema entre PCs
1. Antes de comecar, rodar:
```powershell
git status
git pull --ff-only origin main
```
2. Se mexer em codigo ou config, registrar no `HANDOFF_GLOBAL.md`.
3. Se mexer na planilha `config_incertezas_rev3.xlsx`, tratar isso como mudanca consciente de configuracao e commitar separado quando fizer sentido.
4. Nao usar o Drive como fonte principal do codigo. O codigo deve vir do Git. O Drive fica para dados pesados e backup.
5. Nao apontar o VS Code para Python aleatorio do Windows. Use a `.venv` local do repo.

## Pacotes usados
Pipeline:
- `pandas==3.0.1`
- `numpy==2.4.2`
- `matplotlib==3.10.8`
- `openpyxl==3.1.5`
- `python-calamine==0.6.2`

Viewer rapido:
- `PySide6==6.10.2`
- `pyqtgraph==0.14.0`

## Arquivo de referencia para consulta
- Historico tecnico: `HANDOFF_GLOBAL.md`
- Guia de execucao: `README_EXECUCAO.md`
