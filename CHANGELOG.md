# Changelog

Todas as mudancas relevantes deste repositorio devem ser registradas aqui.

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
