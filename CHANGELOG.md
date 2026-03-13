# Changelog

Todas as mudancas relevantes deste repositorio devem ser registradas aqui.

## 2026-03-13

### Added

- `nanum_pipeline_29.py` criado como copia funcional do estado atual do `nanum_pipeline_28.py`, para a proxima linha de trabalho manter rollback simples no `28`.
- Backend textual novo em `pipeline29_config_backend.py` para substituir a dependencia operacional da planilha `config_incertezas_rev3.xlsx`.
- GUI nova em `pipeline29_config_gui.py` para editar:
  - `Defaults`
  - `data quality`
  - `Mappings`
  - `Instruments`
  - `Reporting_Rounding`
  - `Plots`
- Nova pasta versionada `config/pipeline29_text/`, bootstrapada a partir da `rev3`, contendo:
  - `metadata.toml`
  - `defaults.toml`
  - `data_quality.toml`
  - `mappings.toml`
  - `instruments.toml`
  - `reporting_rounding.toml`
  - `plots.toml`
- Novos mapeamentos de incerteza para temperaturas:
  - `T_S_CIL_1..4`
  - `T_CARTER`
  - `T_AMBIENTE`
  - `T_RADIADOR`
  - `T_S_AGUA`
  - `T_WATERCOOLER`
  - `T_ADMISSAO`
  - `T_E_TURB`
  - `T_S_TURB`
- Novos mapeamentos de incerteza para pressao:
  - `P_S_TURB_RAW`
  - `P_E_TURB_RAW`
  - `P_COLETOR_RAW`
  - `P_S_COMP_RAW`

### Changed

- `nanum_pipeline_28.py` passou a filtrar componentes da aba `Instruments` por seletor da aba `Defaults`, permitindo chavear o `NI9213_TC_MODE` entre modos do modulo.
- `nanum_pipeline_29.py` agora pode carregar configuracao por:
  - `text` (`config/pipeline29_text`)
  - `excel` (`config_incertezas_rev3.xlsx`)
  - `auto` com bootstrap automatico do Excel para texto
- Ao iniciar o `pipeline29` em run normal, o script agora pergunta se a GUI de configuracao deve ser aberta antes do processamento.
- A GUI do `pipeline29` ganhou:
  - `Save` para sobrescrever a config textual atual no mesmo diretorio;
  - `Save As` para gravar em outro diretorio de config;
  - `Variable source` separado para montar catalogo de variaveis via arquivo `.xlsx/.csv`;
  - seletor pesquisavel com wildcard para colunas de `Mappings` (`col_mean`, `col_sd`) e `Plots` (`x_col`, `y_col`, `yerr_col`).
  - helper vertical de `Add row` para `Mappings`, `Instruments` e `Plots`;
  - dropdown editavel de `Instruments.key` alimentado dinamicamente pelas `keys` atuais de `Mappings`, mesmo antes de salvar.
  - abertura maximizada por padrao;
  - sugestao automatica de `col_sd` a partir de `col_mean` no helper de `Mappings`;
  - defaults no helper de `Instruments` para `acc_pct`, `digits`, `lsd` e `resolution` = `0`, com `source = User input`;
  - explicacao visual de `acc_abs` como limite `+/-`;
  - dropdown editavel de `source` com descricao dinamica do catalogo atual de instrumentos;
  - defaults no helper de `Plots` para `enabled = 1`, `plot_type = all_fuels_yx`, `x_col = Load_kW`, `x_min = 0`, `x_max = 55`, `x_step = 5`, com `filename/title` gerados automaticamente e Y em autoscale por padrao.
- O dispatcher de plots do `pipeline29` ganhou a chave `show_uncertainty` por plot:
  - `auto`
  - `on`
  - `off`
- A propagacao de incerteza agora gera tambem:
  - `T_E_CIL_AVG`
  - `DT_ADMISSAO_TO_T_E_CIL_AVG_C`
- O fluxo de runtime ganhou bypass por ambiente com `PIPELINE28_USE_DEFAULT_RUNTIME_DIRS=1`; no `nanum_pipeline_29.py`, o alias novo e `PIPELINE29_USE_DEFAULT_RUNTIME_DIRS=1`, com fallback para a variavel antiga.
- A planilha `config/config_incertezas_rev3.xlsx` foi atualizada para:
  - corrigir `T_AMBIENTE` para termopar tipo `T`;
  - incluir componentes K/T e `NI 9213` por modo;
  - incluir os sensores de pressao com limite `+/-2.93 kPa` tratado como distribuicao retangular;
  - adicionar `yerr_col` nos plots de temperatura e pressao relevantes.

### Validation

- `python -m py_compile nanum_pipeline_28.py`
- `python -m py_compile nanum_pipeline_29.py`
- `python -m py_compile pipeline29_config_backend.py`
- `python -m py_compile pipeline29_config_gui.py`
- Bootstrap real da `rev3` para `config/pipeline29_text/`
- Smoke test da GUI em `.venv` com `QT_QPA_PLATFORM=offscreen`
- Reprocessamento local confirmou colunas `uA/uB/uc/U` para temperaturas e para:
  - `P_S_TURB_RAW`
  - `P_E_TURB_RAW`
  - `P_COLETOR_RAW`
  - `P_S_COMP_RAW`

## 2026-03-13 - Pipeline 29 follow-up: helper editing, fuel properties, plot state and volumetric efficiency

### Added

- `pipeline29_config_gui.py`:
  - duplo clique em linha preenchida de `Mappings`, `Instruments` e `Plots` agora abre o helper vertical para editar a propria linha;
  - botao `Save & Run` para salvar a config textual atual e seguir direto para o processamento do `pipeline29`.
- `pipeline29` passou a usar `config/pipeline29_text/fuel_properties.toml` como fonte textual editavel de:
  - `Fuel_Label`
  - `DIES_pct`
  - `BIOD_pct`
  - `EtOH_pct`
  - `H2O_pct`
  - `LHV_kJ_kg` / PCI
  - `Fuel_Density_kg_m3`
  - `Fuel_Cost_R_L`
- Nova aba `Fuel Properties` na GUI do `pipeline29`.
- Persistencia da ultima selecao do filtro de pontos para plots em:
  - `LOCALAPPDATA\\nanum_pipeline_29\\plot_point_filter_last.json`
- Nova grandeza derivada no `pipeline29`:
  - `ETA_V`
  - `ETA_V_pct`
- Novo plot textual default:
  - `eta_v_pct_vs_power_all.png`

### Changed

- `nanum_pipeline_29.py` agora entende o retorno especial da GUI para `Save & Run`, inclusive quando a config ativa mudou via `Save As`.
- `pipeline29_config_backend.py` passou a carregar/salvar `fuel_properties.toml`, incluindo suporte em presets e fallback para configs antigas sem esse arquivo.
- O bootstrap do texto a partir do legado agora importa tambem o conteudo de `config/lhv.csv` para `fuel_properties.toml`.
- O runtime do `pipeline29` passou a preferir `fuel_properties.toml` e usar `lhv.csv` apenas como fallback legado.
- `build_final_table()` agora usa `Fuel Properties` como fonte principal de:
  - `LHV_kJ_kg`
  - densidade
  - custo
  - `LHV_E94H6_kJ_kg`
- O filtro de pontos para plots, em Qt e Tk, agora:
  - abre carregando automaticamente a ultima selecao salva quando houver compatibilidade com o conjunto atual de pontos;
  - mostra esse estado na propria janela;
  - permite `Carregar ultima` e `Salvar atual`.
- A eficiencia volumetrica foi implementada com:
  - `Rotação_mean_of_windows` como rotacao do motor;
  - cilindrada `3.992 L` via `Defaults`;
  - pressao de referencia fixa de `101.3 kPa` via `Defaults`;
  - temperatura de admissao para calcular a densidade de referencia;
  - `MAF_mean_of_windows` para o diesel `D85B15`;
  - cancelamento do calculo no diesel quando `MAF` estiver estatico ou fora de `0..300 kg/h`.
- A config textual salva pela GUI foi novamente consolidada como fonte da verdade; por isso varios TOMLs foram regravados com os placeholders atuais do editor (`"nan"`, strings numericas, campos `setting_param/setting_value`).

### Config

- `config/pipeline29_text/defaults.toml` ganhou:
  - `ENGINE_DISPLACEMENT_L = 3.992`
  - `VOL_EFF_REF_PRESSURE_kPa = 101.3`
  - `VOL_EFF_RPM_COL = Rotação_mean_of_windows`
  - `VOL_EFF_DIESEL_MAF_COL = MAF_mean_of_windows`
  - `VOL_EFF_DIESEL_MAF_MIN_KGH = 0`
  - `VOL_EFF_DIESEL_MAF_MAX_KGH = 300`
- `config/pipeline29_text/mappings.toml` ganhou o mapping:
  - `ETA_V_pct -> ETA_V_pct`
- `config/pipeline29_text/plots.toml` ganhou o plot:
  - `eta_v_pct_vs_power_all.png`
- `config/pipeline29_text/fuel_properties.toml` ficou como arquivo versionado da configuracao de PCI/LHV, densidade e custo por blend.

### Validation

- `.\.venv\Scripts\python.exe -m py_compile nanum_pipeline_29.py`
- `.\.venv\Scripts\python.exe -m py_compile pipeline29_config_backend.py`
- `.\.venv\Scripts\python.exe -m py_compile pipeline29_config_gui.py`
- Smoke test da persistencia do filtro:
  - salva ultima selecao;
  - recarrega selecao compatibilizando pontos novos.
- Smoke test da eficiencia volumetrica:
  - diesel valido usando `MAF`;
  - diesel invalido cancelado quando `MAF > 300 kg/h`;
  - diesel cancelado quando `MAF` fica estatico.

## 2026-03-12

### Added

- Filtro interativo de pontos para plots no `nanum_pipeline_28.py`, com GUI em `PySide6` e fallback.
- Calculos de economia vs diesel no `LV_KPI`:
  - `Economia_vs_Diesel_R_h`
  - `Economia_vs_Diesel_pct`
  - colunas associadas de baseline diesel e propagacao de incerteza
- Cenarios de maquinas no `LV_KPI`:
  - colheitadeira
  - trator transbordo
  - caminhao
- Plots de cenarios de maquinas no fluxo final do pipeline.
- Entradas novas no `config_incertezas_rev3.xlsx` para horas/ano e consumo diesel das maquinas.
- Linhas novas no `Plots` para:
  - `economia_pct_vs_diesel_power_all.png`
  - `economia_r_h_vs_diesel_power_all.png`

### Changed

- O pipeline passou a abrir o filtro de pontos mais cedo, usando metadata dos arquivos quando possivel.
- O diagnostico de qualidade foi otimizado para reduzir a latencia apos o carregamento dos dados.
- Os plots de cenario de maquinas foram ajustados para:
  - usar `Potencia UPD medida (kW, bin 0.1)` no eixo X
  - legenda no canto superior esquerdo com folga automatica no eixo Y
  - custo horario em `R$/h`
  - economia horaria em `R$/h`
  - consumo anual volumetrico em `x10^3 L/ano`
  - custo anual em `x10^3 R$/ano`
- O pipeline passou a detectar e corrigir automaticamente parametros de maquina invertidos no `Defaults` quando `horas/ano` e `diesel L/h` estiverem trocados.

### Config

- `config/config_incertezas_rev3.xlsx` atualizado com:
  - densidades e custos dos combustiveis
  - custos e densidades revisados para os blends usados
  - parametros de maquinas corrigidos para:
    - `MACHINE_HOURS_PER_YEAR_COLHEITADEIRA = 3150`
    - `MACHINE_DIESEL_L_H_COLHEITADEIRA = 34`
    - `MACHINE_HOURS_PER_YEAR_TRATOR_TRANSBORDO = 1675`
    - `MACHINE_DIESEL_L_H_TRATOR_TRANSBORDO = 12.1`
    - `MACHINE_HOURS_PER_YEAR_CAMINHAO = 4800`
    - `MACHINE_DIESEL_L_H_CAMINHAO = 41`

### Validation

- `python -m py_compile nanum_pipeline_28.py`
- Regeracao local de `lv_kpis_clean.xlsx` e dos plots de cenario para conferir:
  - sinais das economias
  - ordem coerente entre colheitadeira, trator e caminhao
  - labels e escalas dos eixos
