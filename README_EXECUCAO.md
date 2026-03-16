# Execucao e Ambiente

## O que precisa estar no Git
- Codigo principal:
  - `pipeline29_config_backend.py`
  - `pipeline29_config_gui.py`
  - `nanum_pipeline_29.py`
  - `nanum_pipeline_28.py`
  - `standalone_kibox_cycle_viewer_fast.py`
- Config obrigatoria:
  - `config/pipeline29_text/`
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
|   |-- pipeline29_text/
|   |   |-- metadata.toml
|   |   |-- defaults.toml
|   |   |-- data_quality.toml
|   |   |-- mappings.toml
|   |   |-- instruments.toml
|   |   |-- reporting_rounding.toml
|   |   |-- plots.toml
|   |-- lhv.csv
|   |-- rules_consumo.csv
|-- raw/
|-- out/
|-- out_validation/
|-- pipeline29_config_backend.py
|-- pipeline29_config_gui.py
|-- nanum_pipeline_29.py
|-- nanum_pipeline_28.py
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
1. Rode o pipeline normalmente:
```powershell
& ".\.venv\Scripts\python.exe" .\nanum_pipeline_29.py
```
`nanum_pipeline_28.py` fica preservado como rollback do snapshot de 2026-03-13.
Por padrao, o `pipeline29` usa `config/pipeline29_text/` e so cai para o Excel se voce pedir `--config-source excel`.
2. Em toda execucao, o pipeline abre um popup Windows para selecionar:
   - `RAW_INPUT_DIR`
   - `OUT_DIR`
3. O fluxo tenta primeiro o seletor nativo de pastas do Windows; se ele falhar, cai para popup Tkinter e, em ultimo caso, para prompt no terminal.
4. A ultima selecao fica salva localmente em:
   - `%LOCALAPPDATA%\nanum_pipeline_29\pipeline29_runtime_paths.json`
5. O popup volta preenchido com a ultima selecao para acelerar a proxima execucao.
6. O restante do `config/config_incertezas_rev3.xlsx` continua sendo lido normalmente.
7. Apenas `RAW_INPUT_DIR` e `OUT_DIR` sao sincronizados de volta na aba `Defaults`; o resto da planilha nao e alterado.
8. Para rodar sem popup, use `PIPELINE29_USE_DEFAULT_RUNTIME_DIRS=1` antes de chamar o script. O `pipeline29` tambem aceita a variavel legada `PIPELINE28_USE_DEFAULT_RUNTIME_DIRS=1`.

## Como editar a nova configuracao textual
Abrir a GUI:
```powershell
& ".\.venv\Scripts\python.exe" .\pipeline29_config_gui.py
```

Abrir a GUI a partir do proprio pipeline:
```powershell
& ".\.venv\Scripts\python.exe" .\nanum_pipeline_29.py --config-gui
```

Regerar os TOMLs a partir da `rev3`:
```powershell
& ".\.venv\Scripts\python.exe" .\nanum_pipeline_29.py --rebuild-text-config --config-source text
```

Observacoes:
- no run normal, o `pipeline29` pergunta se deve abrir a GUI antes de processar; para automacao, use `--skip-config-gui-prompt` ou `PIPELINE29_SKIP_CONFIG_GUI_PROMPT=1`;
- a GUI salva/carrega presets em JSON fora do repo, em `%LOCALAPPDATA%\nanum_pipeline_29\presets\`;
- o ultimo estado da GUI fica em `%LOCALAPPDATA%\nanum_pipeline_29\config_gui_state.json`;
- a GUI agora tem `Save` e `Save As` separados para a config textual;
- a GUI tem um `Variable source` proprio para gerar catalogo de variaveis e alimentar os seletores de `Mappings` e `Plots`;
- os campos de variavel em `Mappings` e `Plots` aceitam selecao por picker pesquisavel com wildcard ao dar duplo clique na celula;
- `Add row` em `Mappings`, `Instruments` e `Plots` abre helper vertical dedicado;
- o helper de `Instruments` usa dropdown editavel de `key` baseado nas chaves atuais de `Mappings`, inclusive sem salvar;
- a janela principal abre maximizada por padrao;
- no helper de `Mappings`, `col_sd` e sugerido automaticamente a partir do `col_mean`;
- no helper de `Instruments`, `acc_pct`, `digits`, `lsd` e `resolution` caem para `0` quando vazios, `source` assume `User input`, e o `source` mostra descricao dinamica do catalogo atual;
- no helper de `Plots`, os defaults de X ja entram preenchidos (`0 .. 55`, passo `5`) e `filename/title` sao gerados automaticamente pelos eixos;
- cada plot agora pode explicitar `show_uncertainty = auto | on | off` em `config/pipeline29_text/plots.toml`.

## Como rodar os utilitarios KIBOX
Viewer rapido Qt:
```powershell
& ".\.venv\Scripts\python.exe" .\standalone_kibox_cycle_viewer_fast.py
```

Se o `--input` padrao nao existir neste PC, o viewer abre um seletor de arquivo para voce escolher o CSV na hora.

Conversor `.open -> .csv` usando o `OpenToCSV.exe` ja instalado com o KiBox Cockpit:
```powershell
& ".\.venv\Scripts\python.exe" .\kibox_open_to_csv.py "C:\caminho\arquivo.open" --type res --separator tab --name-mode pipeline
```

Observacoes:
- o repositorio canonico do wrapper agora e `https://github.com/Motterdude/kibox_open_to_csv`; a copia dentro de `Processamentos/` continua sendo um espelho operacional para uso direto com o pipeline;
- o wrapper usa `type=res sep=tab cno` por padrao operacional para ficar proximo do formato `_i.csv` que o `pipeline28` ja le;
- para um nome final explicito, por exemplo casar com um `.xlsx`, use `--output-name D85B15_45kW_i.csv`;
- se a entrada for um diretorio, o utilitario varre os `.open` recursivamente e converte um a um.

Interface grafica Windows para selecionar varios `.open`, escolher pasta de saida e acompanhar o log em tempo real:
```powershell
& ".\.venv\Scripts\python.exe" .\kibox_open_to_csv.py --gui
```

Na GUI:
- selecione varios arquivos `.open` ao mesmo tempo;
- selecione a pasta de destino da conversao;
- na primeira abertura, se o `OpenToCSV.exe` nao for encontrado automaticamente, a GUI pede o executavel e salva o caminho em `%LOCALAPPDATA%\nanum_pipeline_28\kibox_open_to_csv_settings.json`;
- nas proximas execucoes, a GUI reutiliza esse caminho salvo e, se voce trocar de computador ou de instalacao, pede o novo `OpenToCSV.exe` sem crashar;
- o nome de saida e sempre `nome_original_i.csv`, para seguir o padrao que o `pipeline28` detecta como KIBOX;
- a GUI usa o arquivo selecionado na lista como amostra visual do lote e monta um dropdown dinamico com pontos de insercao baseados no nome original;
- o dropdown mostra o proprio nome com o texto inserido na posicao escolhida, por exemplo:
  - `NANUM_xxxx_17,5KW-2026-03-06--20-17-31-041.open`
  - `NANUM_17,5KW_xxxx_-2026-03-06--20-17-31-041.open`
- a propria tela mostra a previa final do CSV de saida antes de converter;
- a barra de progresso geral avanca por arquivo concluido e o log mostra a saida do `OpenToCSV` durante a execucao.

## Regras para evitar problema entre PCs
1. Antes de comecar, rodar:
```powershell
git status
git pull --ff-only origin main
```
2. Se mexer em codigo ou config, registrar no `HANDOFF_GLOBAL.md`.
3. Se mexer na planilha `config_incertezas_rev3.xlsx`, tratar isso como mudanca consciente de configuracao e commitar separado quando fizer sentido.
4. O pipeline agora atualiza automaticamente apenas `RAW_INPUT_DIR` e `OUT_DIR` na aba `Defaults` quando voce confirma o popup de execucao.
5. Nao usar o Drive como fonte principal do codigo. O codigo deve vir do Git. O Drive fica para dados pesados e backup.
6. Nao apontar o VS Code para Python aleatorio do Windows. Use a `.venv` local do repo.

## Retomar em casa sem atrito
1. Sincronize o repo:
```powershell
git fetch --all --tags
git checkout main
git pull --ff-only origin main
```
2. Se quiser exatamente o mesmo estado do laboratorio em 2026-03-09:
```powershell
git checkout checkpoint-2026-03-09-lab-sync
# opcional para continuar trabalhando
git switch -c continue-from-lab
```
3. Recrie/atualize a `.venv` deste repo:
```powershell
powershell -ExecutionPolicy Bypass -File .\setup_env.ps1 -WithGui
```
4. Rode o pipeline; o popup vai pedir `RAW_INPUT_DIR` e `OUT_DIR` para o PC atual e ja trazer a ultima selecao preenchida.
5. Valide no log as linhas:
   - `[INFO] RAW_INPUT_DIR (GUI): ...`
   - `[INFO] OUT_DIR (GUI): ...`

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
