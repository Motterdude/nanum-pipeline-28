# HANDOFF_GLOBAL - pipeline27 -> nanum_pipeline_28

Data de consolidacao: 2026-03-05
Projeto: `Processamentos`
Arquivos de referencia:
- `Processamentos/pipeline27.py`
- `Processamentos/nanum_pipeline_28.py`
- `Processamentos/HANDOFF_GLOBAL.md`

## Convencao global de documentacao
- A partir de 2026-03-08, este arquivo passa a ser o documento mestre de handoff do projeto.
- Toda mudanca de codigo, configuracao, dados versionados e estrategia de Git deve ser registrada aqui com data, contexto, arquivos afetados, impacto e pendencias.
- O nome oficial daqui para frente e `HANDOFF_GLOBAL.md`. Nao criar novos arquivos de handoff paralelos sem necessidade objetiva.
- Arquivos grandes de aquisicao bruta em formato `.open` podem ser mantidos localmente para reproducao, mas nao entram no Git comum quando excedem o limite operacional do GitHub. Quando isso acontecer, a decisao deve ser registrada aqui.

## Registro global de mudancas - 2026-03-08
- Origem importada: `D:\Drive\Faculdade\PUC\Mestrado\Dados_Ensaios\Processamento Pyton 28`
- Destino Git: branch `import/notebook-pipeline28-2026-03-08` a partir de `main`
- Objetivo: alinhar este PC com a versao mais nova do `pipeline28` trazida do notebook de trabalho, sem sobrescrever o historico protegido no Git.

### O que foi importado nesta rodada
- `nanum_pipeline_28.py` atualizado a partir da copia do notebook.
- `config/config_incertezas_rev3.xlsx` atualizado.
- Este handoff foi renomeado para `HANDOFF_GLOBAL.md` e promovido a documento mestre do projeto.
- Dados de `raw/PROCESSAR/descendo_aditivado_1` foram alinhados com a copia do notebook para consolidar a nova rodada de ensaio.

### O que nao entra no versionamento Git desta rodada
- `.venv/` foi explicitamente ignorado por ser ambiente local.
- `*.open` foi explicitamente ignorado no Git porque os arquivos da nova rodada de `Descendo_aditivado_1` tem cerca de `151 MB` cada e excedem o limite pratico do GitHub no Git comum.
- Os arquivos `.open` ainda podem existir localmente na pasta de trabalho para rastreabilidade e reproducao operacional, mas ficam fora dos commits.

### Resumo funcional importado do notebook
- Execucao com caminhos relativos ao proprio script (`BASE_DIR`, `RAW_DIR`, `OUT_DIR`, `CFG_DIR`).
- Colunas `Iteracao` e `Sentido_Carga` no output final e nos diagnostics.
- Novo diagnostico ECT com `MAX_ECT_CONTROL_ERROR`, erros absolutos, flags e metricas resumidas.
- Novo calculo de `BSFC_g_kWh` com `uA`, `uB`, `uc` e `U`.
- Limpeza de nomes redundantes como `*_mean_mean_of_windows`.
- Novos plots de comparacao `subida vs descida`, tabela XY nos graficos e suporte a `y_tol_plus` / `y_tol_minus`.
- Atualizacoes de configuracao na `rev3` para emissoes, BSFC, temperaturas e limites de qualidade.

### Status correto dos arquivos de configuracao
- `config/config_incertezas_rev3.xlsx` e a configuracao ativa e autoritativa do `pipeline28`.
- A partir desta limpeza, o codigo carrega somente `config/config_incertezas_rev3.xlsx`.
- Para reduzir ambiguidade operacional, `config/config_incertezas_rev2_renamed.xlsx` foi removido do repositorio em 2026-03-08.
- A pasta `config` fica intencionalmente reduzida ao minimo operacional: `config_incertezas_rev3.xlsx`, `lhv.csv` e `rules_consumo.csv`.

## Atualizacao de runtime paths - 2026-03-08
- A aba `Defaults` de `config/config_incertezas_rev3.xlsx` passou a aceitar dois parametros textuais:
- `RAW_INPUT_DIR`
- `OUT_DIR`
- `RAW_INPUT_DIR` define o diretorio que sera varrido recursivamente para ler os arquivos LabVIEW/Kibox.
- `OUT_DIR` define o diretorio onde os Excel e plots serao gravados.
- Se as celulas ficarem em branco, o comportamento padrao continua:
- `RAW_INPUT_DIR = BASE_DIR/raw/PROCESSAR`
- `OUT_DIR = BASE_DIR/out`
- Os dois parametros aceitam caminho absoluto ou relativo ao diretorio do script.
- O pipeline agora imprime no inicio os caminhos efetivos de `Config`, `Entrada LabVIEW/Kibox` e `Saida`.

## Validacao mestrado - 2026-03-08
- Execucao testada com `RAW_INPUT_DIR = D:\Drive\Faculdade\PUC\Mestrado\Dados_Ensaios\Processamento_Pyton\raw`
- Estrutura observada no mestrado: arquivos `.xlsx` e `_i.csv` diretamente na pasta principal, sem subpastas.
- Resultado do parsing:
- nomes `E65H35`, `E75H25` e `E94H6` foram interpretados corretamente como `EtOH_pct/H2O_pct`;
- `DIES_pct` e `BIOD_pct` permaneceram vazios, como esperado para o conjunto etanol/agua;
- `Iteracao`, `Sentido_Carga` e `SourceFolder` ficam vazios quando nao ha subpastas de contexto.
- A execucao gerou `lv_time_diagnostics.xlsx`, `lv_diagnostics_summay.xlsx` e `lv_kpis_clean.xlsx`, mas falhou no fim da etapa de plots por um bug de codigo: `NameError: table_rows is not defined` em `plot_all_fuels_xy`.
- Correcao aplicada em 2026-03-08: inicializacao de `table_rows` em `plot_all_fuels_xy` para permitir concluir os plots finais em datasets sem subpastas.

## Objetivo deste arquivo
Registrar, para continuidade no Codex, o que ja existia no `pipeline27`, o que foi adicionado no `pipeline28`, e as regras de trabalho herdadas do historico no GPT online.

## Regras de colaboracao herdadas (GPT online)
1. Nao escrever/alterar codigo sem pedido explicito.
2. Sempre trabalhar sobre o ultimo codigo funcional e preservar contagem de linhas do output final.
3. Em alteracoes, devolver arquivo completo (plug-and-play), sem pedir merge manual.
4. Evitar mudar `groupby`/`merge`/filtros sem pedido explicito.
5. Para erros de indice no pandas, preferir `merge/reset_index` (evitar atribuicao direta de serie com MultiIndex).
6. Nao renomear/reordenar colunas existentes sem pedido explicito.
7. Incertezas padronizadas: `uA` (Tipo A), `uB` (Tipo B), `uc` (combinada), `U` (expandida, k=2). Evitar `ut`.
8. Incertezas guiadas por planilha:
- varrer `Mappings` (col_mean e, se existir, col_sd);
- calcular `uB` somente se houver `key` em `Instruments`;
- se nao houver instrumento, deixar `uB/uc/U = NA` (sem default hardcoded);
- permitir multiplas linhas por key em `Instruments` e combinar por RSS.
9. Config principal: rev2/rev3 (`Mappings`, `Instruments`, `Reporting_Rounding`, `Defaults` opcional). Fallback legacy permitido.
10. `Reporting_Rounding` so deve criar `*_report`, sem alterar colunas originais.

## Baseline confirmado no pipeline27 (o que ja existia)
- Leitura LabVIEW `.xlsx`, escolha de aba robusta, `Index`, `WindowID = Index // 30`.
- Leitura Kibox `.csv` robusta, agregacao para colunas `KIBOX_*` e `KIBOX_N_files`.
- Consumo por balanca (`Consumo_kg_h`) por janela/trecho.
- `uB_Consumo_kg_h` via resolucao da balanca (`balance_kg`) quando definida em `Instruments`.
- Agregacao trecho -> ponto (`mean_of_windows`, `sd_of_windows`, `N_trechos_validos`).
- Workflow generico de incerteza orientado por `Mappings` + `Instruments` (`uA/uB/uc/U`).
- Merge com `lhv.csv` e calculo `n_th`, `n_th_pct`, `uc_n_th`, `U_n_th`, `U_n_th_pct`.
- Canais derivados de ar/combustao:
- `Air_kg_h`, `Air_kg_s`, `Air_g_s`;
- umidade absoluta, `cp` ar seco/umido, `hum_ratio_w_kgkg`;
- `DT_ADMISSAO_TO_T_E_CIL_AVG_C` e `Q_EVAP_NET_kW`.
- Calculo `MFB_10_90` a partir de colunas Kibox de AI10/AI90.
- Plots dirigidos por planilha (`Plots`), incluindo `all_fuels_*` e expansao `kibox_all`.

## O que foi adicionado no nanum_pipeline_28
- Suporte a composicao diesel/biodiesel no pipeline inteiro:
- novas chaves `DIES_pct`, `BIOD_pct` (alem de `EtOH_pct`, `H2O_pct`);
- parse de nome com padroes `DxxByy`, `dies*`, `biod*`, e inferencia do complementar (100-x).
- `Load_kW` com float e inferencia por sinal medido (`Carga (kW)`), com quantizacao 0.5 kW.
- Parse de arquivos em subpastas com `BaseName` hierarquico (`pasta__arquivo`).
- Entrada principal mudou para `raw/PROCESSAR/**` (recursivo).
- Resolver de coluna mais robusto (`resolve_col`) com comparacao canonica (ignora acento/case/espacos).
- Merge de composicao mais robusto:
- `_normalized_composition_keys` e `_left_merge_on_fuel_keys`;
- lida com casos diesel x etanol sem quebrar o merge.
- Config:
- `_try_read_sheet` case-insensitive para nome de aba;
- ignora linhas-cabecalho espurias em `Mappings`;
- leitura de `data quality assessment` (limites de qualidade de tempo/controle).
- Diagnostico de qualidade temporal (novo):
- `lv_time_diagnostics.xlsx` por amostra (delta de tempo, flags, erro de controle);
- `lv_diagnostics_summay.xlsx` (resumo por arquivo);
- plots `time_delta_*` globais e por arquivo, com destaque de erro.
- Plots finais e de diagnostico separados por `SourceFolder` (subpasta de origem).
- `make_plots_from_config` agora aceita `mappings` e tenta inferir `yerr_col` automaticamente (`U_*`) quando ausente.
- `compute_trechos_stats` passou a validar grupos via `merge` com grupos validos (evita problemas de MultiIndex).
- `clear_output_dir(OUT_DIR)` no inicio da execucao (limpa outputs anteriores).

## Diferencas de comportamento 27 x 28 (atencao)
- `pipeline27`: procura arquivos somente em `raw/*` (nao recursivo).
- `pipeline28`: procura somente em `raw/PROCESSAR/**` (recursivo).
- `pipeline28` limpa `out/` no inicio; `pipeline27` nao limpava.
- `pipeline28` gera diagnosticos extras de qualidade temporal.

## Funcionalidade que existe no 27 e falta no 28
Nao foi encontrada funcao removida: `nanum_pipeline_28.py` contem todo o nucleo do `pipeline27` e expande funcionalidades.

## Gap entre historico textual e codigo local (importante)
- Historico diz que histogramas KPEAK (4 estilos) estao implementados.
- No codigo local (`pipeline27` e `pipeline28`) nao ha rotinas de histograma KPEAK; existe apenas mensagem informando que continuam fora do workflow.
- Conclusao: tratar KPEAK como pendencia para validacao antes de assumir como pronto.

## Mapa rapido de funcoes-chave no pipeline28
- `parse_meta`: linha ~513
- `read_labview_xlsx`: linha ~623
- `build_time_diagnostics`: linha ~685
- `summarize_time_diagnostics`: linha ~827
- `load_config_excel`: linha ~1211
- `load_lhv_lookup`: linha ~1357
- `compute_trechos_stats`: linha ~1470
- `compute_ponto_stats`: linha ~1520
- `add_uncertainties_from_mappings`: linha ~1573
- `_left_merge_on_fuel_keys`: linha ~1675
- `build_final_table`: linha ~1736
- `make_plots_from_config`: linha ~2197
- `main`: linha ~2474

## Saidas esperadas atuais (pipeline28)
- `out/lv_kpis_clean.xlsx`
- `out/lv_time_diagnostics.xlsx`
- `out/lv_diagnostics_summay.xlsx`
- `out/plots/**` (inclui plots finais e de diagnostico por subpasta)

## Checklist para continuar desenvolvimento com Codex
1. Preservar contagem de linhas do output final (`lv_kpis_clean.xlsx`).
2. Nao quebrar nomenclatura de incerteza (`uA/uB/uc/U`).
3. Nao adicionar bypass hardcoded de incerteza fora de `Instruments`.
4. Em merges de combustivel, manter estrategia por composicao normalizada.
5. Validar cenarios etanol hidratado e diesel/biodiesel.
6. Confirmar se KPEAK deve ser implementado de fato (codigo local ainda nao tem).
7. Revisar se limpar `out/` no inicio e desejado para todos os fluxos.

## Atualizacoes recentes (2026-03-06)
- Robustez de execucao:
- `RAW_DIR`, `OUT_DIR`, `CFG_DIR` passaram a ser relativos ao proprio arquivo `nanum_pipeline_28.py` (`BASE_DIR = Path(__file__).resolve().parent`).
- Com isso, o pipeline pode ser executado de qualquer pasta do terminal sem perder `raw/PROCESSAR`.
- `main()` agora imprime os caminhos efetivos de base e entrada para diagnostico rapido.

- Contexto de ensaio no output final:
- `lv_kpis_clean.xlsx` ganhou colunas `Iteracao` e `Sentido_Carga` derivadas da pasta de origem (`BaseName`).
- Colunas foram movidas para o inicio da planilha (ordem: `Iteracao`, `Sentido_Carga`, depois demais colunas).

- Diagnostics classificados para filtro no Excel:
- `lv_time_diagnostics.xlsx` e `lv_diagnostics_summay.xlsx` tambem recebem `Iteracao` e `Sentido_Carga`.
- Ambos sao ordenados automaticamente por `Iteracao`, `Sentido_Carga` e `Load_kW` (com rank interno de sentido).
- Colunas de classificacao ficam no inicio das planilhas de diagnostics.

- Novo controle termico ECT (agua):
- Implementado monitoramento de controle entre `T_S_AGUA` (ou variante com acento) e `DEM_TH2O`.
- Novo threshold configuravel: `MAX_ECT_CONTROL_ERROR`.
- Novas colunas por amostra em `lv_time_diagnostics.xlsx`:
- `MAX_ECT_CONTROL_ERROR`, `ECT_CTRL_ACTUAL_C`, `ECT_CTRL_TARGET_C`, `ECT_CTRL_ERROR_C`, `ECT_CTRL_ERROR_ABS_C`, `ECT_CTRL_ERROR_FLAG`.
- Novas metricas no sumario `lv_diagnostics_summay.xlsx`:
- `ECT_CTRL_ERRO`, `ECT_CTRL_ERRO_TRANSIENTE`, `ECT_CTRL_ERRO_TRANSIENTE_t_on`, `ECT_CTRL_ERRO_TRANSIENTE_t_off`,
- `ECT_CTRL_ERROR_N`, `ECT_CTRL_ERROR_PCT`, `ECT_CTRL_ERROR_MEAN_ABS_C`, `ECT_CTRL_ERROR_MAX_ABS_C`.
- `DQ_ERROR` agora considera tres blocos: amostragem (`Smp_ERROR`), controle ACT e controle ECT.

- Planilha de configuracao atualizada:
- Arquivo: `config/config_incertezas_rev3.xlsx`.
- Aba: `data quality assessment`.
- Nova linha incluida:
- `param = MAX_ECT_CONTROL_ERROR`
- `value = 2`
- `unit = degC`
- `notes = Erro de controle se |T_S_AGUA - DEM_TH2O| exceder este limite (faixa +-2 degC).`

- Otimizacoes de performance:
- Conversao numerica em `compute_trechos_stats` e `compute_ponto_stats` mudou de loop coluna-a-coluna para atribuicao em bloco (`DataFrame.apply`), reduzindo fragmentacao.
- Frames agregados (`means`, `mean_of_windows`, `sd_of_windows`) passam por `copy()` antes de concatenacao para reduzir blocos fragmentados.
- Resultado pratico: os `PerformanceWarning` de DataFrame altamente fragmentado deixaram de aparecer nas execucoes de validacao.

- Graficos de emissoes adicionados via configuracao (sem hardcode no Python):
- Arquivo editado: `config/config_incertezas_rev3.xlsx`, aba `Plots`.
- Novas linhas (plot_type=`all_fuels_yx`, x_col=`Load_kW`):
- `nox_vs_power_all.png` com `y_col=NOX_mean_of_windows`
- `co_vs_power_all.png` com `y_col=CO_mean_of_windows`
- `co2_vs_power_all.png` com `y_col=CO2_mean_of_windows`
- `o2_vs_power_all.png` com `y_col=O2_mean_of_windows`
- Validado em runtime: os quatro PNGs sao gerados para cada grupo de pasta (`subindo_*` e `descendo_*`) em `out/plots/...`.
- Para novos graficos futuros, o fluxo permanece 100% config-driven: basta adicionar linhas na aba `Plots` (enabled, plot_type, filename, x_col, y_col, labels e eixos).

- Observacao operacional:
- O pipeline continua limpando `out/` no inicio. Se algum arquivo Excel de `out/` estiver aberto, a execucao aborta com mensagem de arquivo em uso.

## Atualizacoes recentes (2026-03-06 - parte 2)
- Controle ECT com tolerancia de projeto `+-2 degC`:
- `DEFAULT_MAX_ECT_CONTROL_ERROR_C` mudou de `5.0` para `2.0`.
- Na planilha `config/config_incertezas_rev3.xlsx` (aba `data quality assessment`), `MAX_ECT_CONTROL_ERROR` foi fixado em `2`.
- Em `lv_time_diagnostics.xlsx`, alem de erro/flag, agora saem os limites absolutos por amostra:
- `ECT_CTRL_LIMIT_LOW_C = DEM_TH2O - 2`
- `ECT_CTRL_LIMIT_HIGH_C = DEM_TH2O + 2`
- Mantido sinal de erro: `ECT_CTRL_ERROR_C = T_S_AGUA - DEM_TH2O` (positivo = motor mais quente que o setpoint).

- Consumo especifico (BSFC) com incerteza composta:
- Nova coluna no output final `lv_kpis_clean.xlsx`: `BSFC_g_kWh` (`1000 * Consumo_kg_h / Potencia_kW`).
- Novas incertezas propagadas para BSFC:
- `uA_BSFC_g_kWh` (parte tipo A de potencia + vazao),
- `uB_BSFC_g_kWh` (parte tipo B de potencia + vazao),
- `uc_BSFC_g_kWh`,
- `U_BSFC_g_kWh` (k=2).
- A potencia usa o canal mapeado `power_kw` + instrumentos (`Instruments`), e a vazao usa `Consumo_kg_h` com `uB` da balanca (`balance_kg`) + `uA` por repeticao de trechos.

- Plots rev3 (config-driven):
- Aba `Plots` ganhou o grafico `bsfc_vs_power_all.png` (`x_col=Load_kW`, `y_col=BSFC_g_kWh`, `yerr_col=U_BSFC_g_kWh`).
- Colunas `y_tol_plus` e `y_tol_minus` seguem ativas no workflow:
- `0/0` desabilita linhas-guia;
- valores positivos desenham limites horizontais em vermelho.

## Atualizacoes recentes (2026-03-06 - limpeza de nomes estatisticos)
- Padronizacao de nomes para remover repeticao de estatistica no output final:
- `*_mean_mean_of_windows` -> `*_mean_of_windows`
- `*_mean_sd_of_windows` -> `*_sd_of_windows`
- A limpeza passou a ser aplicada na agregacao por ponto (`compute_ponto_stats`), evitando gerar nomes redundantes na origem.

- Compatibilidade com configuracoes antigas:
- `resolve_col` agora aceita referencias legadas com nome repetido e resolve automaticamente para o nome novo (ex.: `NOX_mean_mean_of_windows` -> `NOX_mean_of_windows`).

- Configuracoes Excel atualizadas:
- `config/config_incertezas_rev3.xlsx` atualizado nas abas `Mappings` e `Plots` para usar somente nomes limpos.
- `filename` da aba `Plots` tambem foi normalizado (ex.: `raw_*_mean_of_windows_vs_power_all.png`).
- `config/config_incertezas_rev2_renamed.xlsx` tambem recebeu ajuste em `Mappings` para manter fallback sem nomes repetidos.

## Atualizacoes recentes (2026-03-06 - novos plots de temperatura por carga)
- Adicionados no `config/config_incertezas_rev3.xlsx` (aba `Plots`) quatro novos graficos `all_fuels_yx`, todos com `x_col=Load_kW`:
- `t_watercooler_vs_power_all.png` (`y_col=T_WATERCOOLER_mean_of_windows`)
- `t_radiador_vs_power_all.png` (`y_col=T_RADIADOR_mean_of_windows`)
- `t_carter_vs_power_all.png` (`y_col=T_CARTER_mean_of_windows`, `yerr_col=U_T_CARTER_C`)
- `t_ambiente_vs_power_all.png` (`y_col=T_AMBIENTE_mean_of_windows`, `yerr_col=U_T_AMBIENTE_C`)

## Atualizacoes recentes (2026-03-06 - mapa de dependencias de potencia)
- Mapeamento atual das contas (importante para futuras mudancas de eixo):
- `Load_kW` (potencia aplicada/comando) esta sendo usado para:
- agrupamento de trechos e ponto (`compute_trechos_stats` / `compute_ponto_stats`);
- merge com Kibox no final;
- eixo X padrao da maioria dos plots da aba `Plots`.

- `Potencia Total` (UPD medido) esta sendo usado para:
- calculo de `n_th` / `n_th_pct` e suas incertezas (via `Mappings.key=power_kw`);
- calculo de `BSFC_g_kWh` e incertezas de BSFC (`uA/uB/uc/U_BSFC_g_kWh`).

- Estado da planilha rev3 neste momento:
- `Mappings.power_kw.col_mean = Potencia Total_mean_of_windows`;
- portanto, as contas fisicas de eficiencia/consumo especifico usam potencia medida real.

- Observacao de consistencia para proxima alteracao:
- o valor de `BSFC_g_kWh` ja usa `Potencia Total` (UPD), mas o grafico `bsfc_vs_power_all.png` ainda esta configurado com `x_col=Load_kW`.
- Nao foi mudado ainda o eixo deste plot para potencia medida; ficou pendente para pedido explicito.

## Atualizacoes recentes (2026-03-06 - pasta compare com subida+descida)
- Mantidos os plots separados por pasta de origem (`subindo_*` e `descendo_*`).
- Adicionada terceira saida automatica em `out/plots/compare/` com os mesmos plots, combinando subida e descida para comparacao direta.

- Regras de agrupamento para comparacao:
- O codigo identifica automaticamente os pares a partir do nome da pasta de origem.
- Tokens de direcao (`subindo`, `subida`, `descendo`, `descida`, `up`, `down`) sao removidos para formar a chave de comparacao.
- Exemplo atual: `subindo_aditivado_1` + `Descendo_aditivado_1` -> `out/plots/compare/aditivado_1/`.
- A logica nao depende de nomes hardcoded e funciona para novas iteracoes seguindo o mesmo padrao de nome.

- Legenda dos plots compare:
- Cada curva passa a carregar a origem (`SourceFolder`) + combustivel (`H2O=...`), permitindo distinguir subida e descida no mesmo grafico.

## Atualizacoes recentes (2026-03-06 - ajuste fino compare + tabela XY + sem yerr em T_Carter/T_Ambiente)
- Nome da pasta compare ajustado para o formato solicitado:
- `<nome_subida> vs <nome_descida>`.
- Exemplo atual: `out/plots/compare/subida_aditivado_1 vs descida_aditivado_1/`.
- A normalizacao converte automaticamente `subindo -> subida` e `descendo -> descida` no nome da pasta compare.

- Tabela de valores X/Y nos graficos:
- Implementada tabela compacta no canto direito para os graficos gerados em `out/plots` (plots de configuracao e plots de diagnostico de tempo).
- A tabela mostra pares `X` e `Y` (e coluna `Serie` quando houver multiplas curvas), com limite de linhas para manter legibilidade.

- T_CARTER e T_AMBIENTE sem barras de incerteza no plot:
- Na aba `Plots` da rev3, `yerr_col` desses dois graficos foi setado para `off`.
- O codigo agora reconhece tokens de desabilitacao de incerteza (`off`, `none`, etc.) e nao faz auto-inferencia de `yerr` nesses casos.
- As contas de incerteza continuam ativas no `lv_kpis_clean.xlsx` (`uA/uB/uc/U` preservados).

- Limpeza de output:
- `clear_output_dir` passou a remover tambem diretorios vazios, evitando sobras de pastas antigas em `out/plots/compare`.
