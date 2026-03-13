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
