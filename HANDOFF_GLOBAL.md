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

## Modo mestrado para eixo X de plots - 2026-03-08
- Objetivo: no conjunto do mestrado, os plots finais nao devem usar `Load_kW` como eixo X padronizado; devem usar a potencia real medida no UPD.
- Regra de ativacao: esse modo so entra em efeito quando o processo e executado com `cwd` dentro de `D:\Drive\Faculdade\PUC\Mestrado`, independentemente da subpasta.
- Implementacao:
- foi criado o detector `is_mestrado_runtime()` baseado em `Path.cwd()`;
- a tabela final passa a carregar `UPD_Power_kW` (valor medio medido de potencia) e `UPD_Power_Bin_kW` (mesmo valor arredondado para `0.1 kW`);
- nos plot types dirigidos pela planilha (`all_fuels_yx`, `all_fuels_xy`, `all_fuels_labels`, `kibox_all`), toda requisicao vazia ou explicita de `Load_kW` passa a ser resolvida como `UPD_Power_Bin_kW` quando o runtime e do mestrado;
- a label do eixo X tambem passa a ser forcada para `Potencia UPD medida (kW, bin 0.1)` quando a planilha usar labels genericas como `Load_kW`, `Carga (kW)` ou `Power (kW)`.
- Validacao executada em 2026-03-08:
- a pasta `out` original do mestrado estava parcialmente bloqueada por um Excel aberto, entao a validacao foi rodada em um diretorio temporario de saida: `D:\Drive\Faculdade\PUC\Mestrado\Dados_Ensaios\Processamento_Pyton\out_runtime_test_134506`;
- a execucao concluiu com `101` plots de configuracao, `30` plots de `time_delta_by_file` e os tres Excel finais (`lv_time_diagnostics.xlsx`, `lv_diagnostics_summay.xlsx`, `lv_kpis_clean.xlsx`);
- o `lv_kpis_clean.xlsx` gerado nessa validacao manteve `30` linhas e passou a expor os bins reais do UPD, por exemplo `4.8`, `9.6`, `14.3`, `14.4`, `19.1`, `23.8`, `30.8`, `30.9`, `35.6`, `40.2`, `44.9`, `45.0`, `49.1`, `49.4`;
- confirmacao visual feita em `nth_vs_power_all.png`: os pontos e a tabela XY usam a potencia medida do UPD, e o eixo X ficou rotulado como `Potencia UPD medida (kW, bin 0.1)`.

## Correcao de carga nominal, KIBOX e limpeza visual - 2026-03-08
- Problema observado no dataset do mestrado quando o script era executado fora do `cwd` do mestrado:
- os plots principais em `Load_kW` paravam em `47.5`, porque os ensaios nominais de `45` e `50 kW` estavam sendo colapsados pelo sinal inferido da coluna de carga;
- o merge do KIBOX desaparecia de `30` a `50 kW`, porque o LabVIEW estava com `Load_kW = 32.5/37.5/42.5/47.5`, enquanto o KIBOX continuava com a carga nominal do nome do arquivo (`30/35/40/45/50`);
- as tabelas embutidas nos plots ficaram visualmente ruins e foram removidas.
- Causa raiz:
- `read_labview_xlsx()` estava sobrescrevendo `Load_kW` com a carga inferida do sinal quando o valor divergisse mais de `0.75 kW` do nome do arquivo;
- no mestrado, esse sinal inferido fica coerente ate `25 kW`, mas acima disso assume degraus `32.5/37.5/42.5/47.5` e nao deve ser usado como identificador nominal do ensaio.
- Correcao aplicada:
- `Load_kW` voltou a representar a carga nominal/indexada do nome do arquivo sempre que essa informacao existir;
- a carga inferida do sinal agora e preservada separadamente em `Load_Signal_kW` e, apos agregacao, aparece como `Load_Signal_kW_mean_of_windows`;
- o merge do KIBOX voltou a casar por `Load_kW` nominal + composicao, recuperando os dados de `30`, `35`, `40`, `45` e `50 kW`;
- `_add_xy_value_table()` foi desativada, removendo as tabelas de todos os graficos.
- Validacao executada em 2026-03-08 no output temporario `D:\Drive\Faculdade\PUC\Mestrado\Dados_Ensaios\Processamento_Pyton\out_repo_mode_135955`:
- `lv_kpis_clean.xlsx` voltou a ter `Load_kW = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50]`;
- `Load_Signal_kW_mean_of_windows` preservou o comportamento anomalo do sinal (`32.5`, `37.5`, `42.5`, `47.5`) para diagnostico, sem contaminar o identificador nominal;
- `rows with any kibox = 30`, ou seja, o KIBOX voltou para todas as linhas do ensaio;
- confirmacao visual feita em `t_radiador_vs_power_all.png`: sem tabela embutida e com pontos ate `50 kW` sem colapso em `47.5`.

## Limpeza de duplicidades KIBOX e housekeeping de debug - 2026-03-08
- Problema residual observado apos a correcao principal:
- alguns CSVs do KIBOX traziam pares de colunas equivalentes, por exemplo `AI50_AVG_1` e `AI50_AVG__1`, `APMAX_AVG_1` e `APMAX_AVG__1`, `IMEPH_AVG_1` e `IMEPH_AVG__1`, `IMEPN_AVG_1` e `IMEPN_AVG__1`, `PMAX_AVG_1` e `PMAX_AVG__1`;
- isso gerava duas colunas semanticamente iguais no output final e fazia o `kibox_all` tentar salvar o mesmo plot mais de uma vez.
- Correcao aplicada:
- `read_kibox_csv_robust()` agora consolida colunas equivalentes apos a leitura, colapsando nomes repetidos por normalizacao simples e preservando o primeiro valor nao nulo por linha;
- `make_plots_from_config()` passou a proteger a expansao `kibox_all` contra colisao de `filename` ja visto na mesma rodada.
- Validacao direta na agregacao KIBOX:
- `kibox_rows = 30`;
- `loads = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50]`;
- `dup_groups = 0` apos a consolidacao.
- Housekeeping operacional:
- os diretorios temporarios de validacao (`out_repo_mode_*`, `out_runtime_test_*`) e logs de runtime criados para debug local devem ser removidos ao final da verificacao;
- nesta rodada, os residuos temporarios usados na analise foram efetivamente apagados da pasta `D:\Drive\Faculdade\PUC\Mestrado\Dados_Ensaios\Processamento_Pyton`.

## Integracao MOTEC CSV - 2026-03-08
- Novo tipo de fonte suportado: arquivos MOTEC em `.csv` com sufixo `_m`, por exemplo `50KW_E94H6_2_m.csv`.
- Regras de identificacao do nome:
- a carga nominal continua sendo lida no inicio do nome (`50KW`);
- a composicao etanol/agua segue o mesmo parse dos arquivos LabVIEW (`E94H6`);
- o sufixo `_m` marca a origem MOTEC e ativa a leitura dedicada.
- Estrutura do arquivo:
- linhas `1..14`: metadados;
- linha `15`: nomes das variaveis;
- linha `16`: unidades;
- dados a partir da linha `17`.
- Implementacao:
- `parse_meta()` passou a classificar `_m.csv` como `source_type = MOTEC`;
- `read_motec_csv()` le o header da linha `15`, descarta a linha de unidades, prefixa todas as variaveis com `Motec_` e adiciona `Motec_SampleRate_Hz`, `Motec_Duration_s` e `Motec_Time_Delta_s`;
- a leitura usa fallback de codificacao para `latin-1`, necessario para os CSVs reais da MOTEC observados no mestrado;
- o processamento da MOTEC usa blocos de `30` amostras (`SAMPLES_PER_WINDOW = 30`), mas sem assumir `DT_S = 1.0`; a taxa real fica preservada nas colunas `Motec_SampleRate_Hz_*` e `Motec_Time_Delta_s_*`;
- `compute_motec_trechos_stats()` agrega medias por bloco valido;
- `compute_motec_ponto_stats()` gera `Motec_*_mean_of_windows`, `Motec_*_sd_of_windows`, `Motec_N_trechos_validos`, `Motec_N_files` e `Motec_N_samples_mean_of_windows`;
- o merge final com o output do pipeline e feito por `Load_kW` nominal + composicao, sem depender do `BaseName` exato do arquivo MOTEC.
- Validacao executada em 2026-03-08:
- leitura unitaria confirmada em `50KW_E94H6_2_m.csv`: `2962` linhas de dados, `99` janelas de `30` amostras, `Motec_SampleRate_Hz = 10.0` e `Motec_Time_Delta_s = 0.1`;
- agregacao MOTEC no conjunto do mestrado: `10` arquivos, `22960` linhas brutas, `764` trechos validos e `10` pontos agregados (`Load_kW = 5..50`);
- no merge final em memoria, o output ficou com `30` linhas totais e `209` colunas prefixadas com `Motec_`;
- os dados MOTEC entraram corretamente nas linhas `E94H6` de `5` a `50 kW`, com `Motec_N_trechos_validos = 74` na maioria dos arquivos e `98` para `50 kW`.

## Novos plots MOTEC via Excel - 2026-03-08
- Mudanca feita exclusivamente pela aba `Plots` de `config/config_incertezas_rev3.xlsx`, mantendo a estrutura de configuracao por planilha.
- Plot adicionado:
- `motec_lambda_vs_power_all.png`
- `plot_type = all_fuels_yx`
- `x_col = Load_kW`
- `y_col = Motec_Exhaust Lambda_mean_of_windows`
- `yerr_col = off`
- eixo Y fixo em `0.90 .. 1.10` com passo `0.02`
- label Y: `Lambda (-)`
- Plot adicionado:
- `motec_ignition_advance_vs_power_all.png`
- `plot_type = all_fuels_yx`
- `x_col = Load_kW`
- `y_col = Motec_Ignition Timing_mean_of_windows`
- `yerr_col = Motec_Ignition Timing_sd_of_windows`
- eixo Y em autoscale
- label Y: `Ignition advance (deg BTDC)`
- o titulo tambem explicita `BTDC` para deixar claro o significado do valor plotado.
- Validacao:
- as duas linhas foram confirmadas na aba `Plots`;
- as colunas `Motec_Exhaust Lambda_mean_of_windows`, `Motec_Ignition Timing_mean_of_windows` e `Motec_Ignition Timing_sd_of_windows` existem no `lv_kpis_clean.xlsx` real do mestrado;
- nenhuma pasta temporaria de debug foi criada nesta alteracao, porque a verificacao foi apenas estrutural na planilha e no output existente.

## Ajuste de incerteza no plot MOTEC de lambda - 2026-03-08
- O plot `motec_lambda_vs_power_all.png` deixou de usar `yerr_col = off`.
- A partir desta revisao, o plot usa `yerr_col = Motec_Exhaust Lambda_sd_of_windows`.
- Decisao explicitada: por agora, a barra de erro do lambda deve refletir apenas o desvio padrao entre trechos, sem incorporar incerteza de equipamento.
- O restante da configuracao foi mantido:
- `x_col = Load_kW`
- `y_col = Motec_Exhaust Lambda_mean_of_windows`
- eixo Y fixo `0.90 .. 1.10`
- passo `0.02`
- Validacao:
- a linha correspondente na aba `Plots` foi confirmada apos a edicao;
- as colunas `Motec_Exhaust Lambda_mean_of_windows` e `Motec_Exhaust Lambda_sd_of_windows` existem no output real do mestrado.

## Plot MOTEC de fuel closed loop trim - 2026-03-08
- Plot adicionado exclusivamente via aba `Plots` em `config/config_incertezas_rev3.xlsx`.
- Arquivo:
- `motec_fuel_closed_loop_trim_vs_power_all.png`
- Configuracao:
- `plot_type = all_fuels_yx`
- `x_col = Load_kW`
- `y_col = Motec_Fuel Closed Loop Control Bank 1 Trim_mean_of_windows`
- `yerr_col = Motec_Fuel Closed Loop Control Bank 1 Trim_sd_of_windows`
- eixo Y em autoscale
- label Y: `Fuel closed loop trim bank 1 (%Trim)`
- Criterio de incerteza:
- a barra de erro usa somente o desvio padrao entre trechos, sem incerteza de equipamento.
- Validacao:
- a nova linha foi confirmada na aba `Plots`;
- as colunas `Motec_Fuel Closed Loop Control Bank 1 Trim_mean_of_windows` e `Motec_Fuel Closed Loop Control Bank 1 Trim_sd_of_windows` existem no `lv_kpis_clean.xlsx` real do mestrado;
- nenhuma pasta temporaria de debug foi criada nesta alteracao.

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

## Ajuste de escala do plot MOTEC de ignition advance - 2026-03-08
- O plot `motec_ignition_advance_vs_power_all.png` passou a usar marcacoes de eixo Y de `1 em 1 grau`.
- A escala do Y continua em `autoscale`; apenas o espacamento dos ticks foi forzado para `1 deg`.
- Para suportar isso sem travar `y_min/y_max`, `make_plots_from_config()` passou a aceitar `y_step` isolado como configuracao de ticks quando `fixed_y` nao esta definido.
- A aba `Plots` da `config_incertezas_rev3.xlsx` foi atualizada com `y_step = 1` para esse plot.

## Utilitario standalone para PCYL_1 e Q_1 por crank angle - 2026-03-08
- Criado o script separado `standalone_kibox_cycle_plots.py`, sem integracao no `pipeline28`.
- O utilitario le um unico CSV do KIBOX no formato convertido por aba, usando:
  - separador `tab`;
  - decimal com virgula;
  - segunda linha de unidades ignorada.
- `Cycle number` e preenchido com `ffill`, porque o arquivo so marca explicitamente a troca de ciclo no primeiro ponto de cada novo ciclo.
- O processamento faz duas etapas:
  - media por `CycleNumber + CrankAngle_deg` para consolidar pontos repetidos dentro do mesmo ciclo;
  - media final por `CrankAngle_deg` entre todos os ciclos.
- Saidas default:
  - input: `TESTE_50KW_E100-2026-01-17--17-12-46-081.csv`;
  - output: `F:\temporario`.
- O script gera:
  - `*_pcyl_mean_vs_crank_angle.png` com janela `-40 a 80 deg CA`;
  - `*_q1_mean_vs_crank_angle.png` com janela `-30 a 90 deg CA`;
  - `*_mean_curves.csv` com curvas medias e contagem de ciclos por angulo.

## Ajuste do utilitario standalone para medias por bloco de 30 ciclos - 2026-03-08
- O utilitario `standalone_kibox_cycle_plots.py` deixou de fazer uma unica media global entre todos os ciclos.
- Agora ele organiza os dados em blocos de ciclos, com default `30 ciclos por bloco`.
- Para cada bloco:
  - faz a media de `PCYL_1` por `CycleNumber + CrankAngle_deg`;
  - faz a media final por `CrankAngle_deg` dentro daquele bloco.
- Os plots passam a mostrar uma curva por bloco, com legenda no formato `Cycles 1-30`, `Cycles 31-60`, etc.
- O CSV de saida foi trocado para `*_cycle_block_mean_curves.csv`, contendo:
  - `CycleBlockIndex`;
  - `CycleBlockStart`;
  - `CycleBlockEnd`;
  - `CycleBlockLabel`;
  - `CrankAngle_deg`;
  - medias/desvios por bloco para `PCYL_1` e `Q_1`.
- O tamanho do bloco pode ser alterado via `--cycle-block-size`, mas o default usado no pedido foi mantido em `30`.

## Otimizacao do standalone para evitar travamento em drop_duplicates - 2026-03-08
- A primeira versao por blocos ainda fazia `drop_duplicates()` em milhoes de linhas para reconstruir o mapa `CycleNumber -> bloco`.
- Isso travava ao rodar o script diretamente no arquivo de `50 kW`.
- A solucao foi mover o calculo de bloco para depois da media por `CycleNumber + CrankAngle_deg`.
- Com isso:
  - o bloco passa a ser calculado em cima da tabela `per_cycle`, muito menor;
  - deixa de existir `drop_duplicates()` na massa bruta;
  - o run standalone direto volta a terminar em tempo normal.

## Plots 3D adicionais no standalone por bloco de ciclos - 2026-03-08
- O utilitario `standalone_kibox_cycle_plots.py` ganhou dois plots adicionais em pseudo-3D, sem remover os plots 2D existentes.
- Novas saidas:
  - `*_pcyl_mean_vs_crank_angle_3d.png`;
  - `*_q1_mean_vs_crank_angle_3d.png`.
- Estrutura dos eixos nesses plots:
  - `x`: `Crank angle`;
  - `y`: valor medio (`P_CYL` ou `Q_1`);
  - `z`: posicao do bloco de ciclos.
- Cada curva 3D representa um bloco medio de ciclos e usa o mesmo agrupamento de `30 ciclos por bloco`.
- O eixo `z` usa labels no formato do bloco (`1-30`, `31-60`, etc.) para facilitar leitura lateral da evolucao ao longo dos conjuntos.
- A visualizacao foi fixada com angulo lateral (`view_init`) para priorizar a comparacao do formato da curva entre blocos.

## Viewer interativo ciclo a ciclo com slider - 2026-03-08
- Criado o utilitario separado `standalone_kibox_cycle_viewer.py`.
- Ele usa `matplotlib + Slider` para navegar ciclo a ciclo no mesmo arquivo do KIBOX, sem integrar nada ao `pipeline28`.
- Estrutura da tela:
  - subplot superior: `PCYL_1`;
  - subplot inferior: `Q_1`;
  - terceiro subplot: `PMAX` por ciclo;
  - slider inferior para escolher o `CycleNumber`.
- Comportamento:
  - a escala dos eixos fica fixa entre ciclos, para permitir comparacao direta;
  - o grafico pode sobrepor a media do bloco de `30 ciclos` como referencia;
  - setas `left/right` do teclado avancam ou recuam um ciclo;
  - o subplot de `PMAX` tem cursor vertical sincronizado com o ciclo selecionado;
  - o ponto correspondente ao `PMAX` do ciclo selecionado e destacado;
  - o canto direito do slider foi trocado por uma caixa de entrada (`TextBox`) para digitar o ciclo desejado.
- Reaproveitamento de logica:
  - o viewer importa `load_cycle_dataframe` e `mean_curve_by_cycle_block` do utilitario `standalone_kibox_cycle_plots.py`, para manter o mesmo parse do CSV.
- Calculo de `PMAX`:
  - `PMAX` e calculado ciclo a ciclo a partir do maior valor de `PCYL_1` encontrado naquele ciclo.
- Parametros uteis:
  - `--initial-cycle`;
  - `--cycle-block-size`;
  - `--hide-block-mean`;
  - `--no-show` para validar carregamento sem abrir a janela.
- Backend:
  - `Agg` quando usado com `--no-show`;
  - `TkAgg` se o `Tk` estiver funcional no Python local;
  - `WebAgg` como fallback interativo quando o `Tk` estiver quebrado, abrindo o viewer via navegador local.

## Otimizacao de performance do viewer interativo - 2026-03-08
- O viewer ficou mais rapido tanto na abertura quanto na resposta do slider.
- Mudancas principais:
  - `PCYL_1` e `Q_1` passam a ser agregados em uma unica tabela `per_cycle`, em vez de dois `groupby` independentes sobre a base bruta;
  - os dados de ciclo e bloco passam a ser guardados como arrays `numpy`, e nao mais como `DataFrame` por ciclo;
  - `PMAX` do ciclo selecionado passou a ser buscado em dicionario (`dict`) em vez de filtro em `DataFrame` a cada movimento;
  - o `TextBox` do ciclo e atualizado sem disparar callbacks extras quando o slider se move;
  - o viewer ignora eventos redundantes quando o slider nao muda de ciclo de fato.
- Medicao local nesta revisao:
  - inicializacao com `--no-show --initial-cycle 150` caiu de aproximadamente `7.8 s` para `6.0 s`.
- Observacao:
  - nao foi usada paralelizacao por nucleos para a interface, porque o gargalo principal era estrutura de dados + redraw do `matplotlib`, e a UI continua essencialmente single-thread.

## Viewer rapido em PyQtGraph/Qt - 2026-03-08
- Como o arrasto do slider no `matplotlib + WebAgg` continuou lento, foi criado um viewer rapido separado: `standalone_kibox_cycle_viewer_fast.py`.
- Stack adotada:
  - `PySide6`;
  - `pyqtgraph`.
- Dependencias instaladas na `.venv` do mestrado:
  - `PySide6 6.10.2`;
  - `pyqtgraph 0.14.0`.
- Para reprodutibilidade no repo, foi criado `requirements_gui_viewer.txt` com essas dependencias opcionais do viewer rapido.
- O viewer rapido preserva a mesma estrutura funcional:
  - `PCYL_1` ciclo a ciclo;
  - `Q_1` ciclo a ciclo;
  - `PMAX` por ciclo;
  - cursor vertical sincronizado em `PMAX`;
  - caixa `Go to`;
  - sobreposicao opcional da media do bloco.
- Diferenca principal:
  - a renderizacao e o controle do slider passam a ser feitos em Qt nativo, sem `matplotlib` e sem `WebAgg`, o que melhora muito a fluidez do arrasto.
- Validacao feita:
  - `py_compile` do arquivo;
  - `--no-show`;
  - instanciacao real do widget com `QT_QPA_PLATFORM=offscreen`, confirmando inicializacao do viewer com ciclo `150`.

## Refinos visuais e Open CSV no viewer rapido - 2026-03-08
- O `standalone_kibox_cycle_viewer_fast.py` foi refinado para deixar a visualizacao mais limpa e mais pratica.
- Ajustes visuais:
  - espessura das linhas reduzida nos tres graficos;
  - `antialias=True` no `pyqtgraph` para melhorar a nitidez da renderizacao;
  - janela inicial ampliada para `1650x1050`, melhorando a leitura do conjunto todo.
- Open file:
  - adicionado botao `Open CSV` dentro da propria aplicacao;
  - a selecao de arquivo usa `QFileDialog`;
  - ao abrir um novo CSV, o viewer recarrega os dados na mesma janela, sem precisar fechar e relancar o script.
- Estrutura tecnica:
  - foi criado `prepare_viewer_dataset(...)` para centralizar o parse e permitir recarga limpa de qualquer CSV compatível.
- Validacao:
  - `py_compile`;
  - `--no-show`;
  - smoke test em `offscreen`, confirmando criacao do botao `Open CSV`, label do arquivo e inicializacao normal do widget.

## Comparacao de ate 3 arquivos e exportacao no viewer rapido - 2026-03-08
- O `standalone_kibox_cycle_viewer_fast.py` ganhou uma aba `Compare`, separada do viewer principal.
- Estrutura nova da aba:
  - dois graficos comparativos: `PCYL_1` e `Q_1`;
  - ate `3` slots independentes de comparacao;
  - cada slot permite:
    - carregar um CSV proprio;
    - limpar o slot;
    - escolher `Cycle` ou `Block mean`;
    - informar um `Cycle ref`, usado diretamente no modo `Cycle` e como referencia para localizar o bloco no modo `Block mean`.
- Para agilizar o fluxo, foi adicionado um botao `Copy Current to Slot 1`, que copia o CSV aberto no viewer principal para o primeiro slot da comparacao.
- Legendas e titulos:
  - a legenda mostra `nome_do_arquivo | Cycle N` ou `nome_do_arquivo | Mean A-B`;
  - curvas `Block mean` aparecem tracejadas;
  - curvas `Cycle` permanecem solidas.
- Exportacao:
  - botao `Export Compare`;
  - o usuario escolhe apenas o diretorio de destino;
  - o export salva:
    - `..._pcyl.png`;
    - `..._q1.png`;
    - `..._selection.csv` com `slot`, `csv_path`, `mode`, `cycle_reference`, `selected_cycle`, `block_label` e `summary`.

## Reducao de uso de memoria no viewer rapido - 2026-03-08
- A preparacao do dataset do viewer rapido foi ajustada para evitar o `groupby` global por `CycleNumber + CrankAngle_deg` sobre arquivos grandes de combustao.
- Motivo:
  - os CSVs KIBOX usados nesta rotina ja chegam com um unico ponto por combinacao `ciclo + angulo de manivela`;
  - o `groupby` global nao reduzia os dados e ainda estourava memoria em arquivos grandes.
- Ajuste aplicado:
  - `build_per_cycle_means(...)` passou a apenas ordenar e normalizar os dados, sem agrupar globalmente;
  - a media por bloco continua sendo calculada em cima do `CycleBlockIndex`, que e onde a agregacao realmente faz sentido.
- Efeito:
  - o viewer voltou a abrir o arquivo grande `TESTE_50KW_E100-2026-01-17--17-12-46-081.csv` com estabilidade;
  - isso tambem deixa o caminho preparado para comparar ate `3` arquivos sem desperdiçar RAM logo na carga inicial.

## Ajuste de selecao do Block mean e export 4:3 no viewer rapido - 2026-03-08
- A aba `Compare` do `standalone_kibox_cycle_viewer_fast.py` foi corrigida no modo `Block mean`.
- Causa do problema reportado:
  - o controle numerico ainda operava como `Cycle ref`, entao mudar de `2` para `3`, por exemplo, continuava apontando para ciclos dentro do mesmo primeiro bloco, o que fazia parecer que o `Block mean` nao atualizava.
- Correcao aplicada:
  - no modo `Block mean`, o controle passa a operar como `Block idx`;
  - o range passa a ser `1..N_blocos`;
  - a curva carregada passa a vir diretamente do indice do bloco, e nao mais de um ciclo de referencia.
- Efeito prático:
  - ao trocar `Block idx`, a curva comparativa muda imediatamente;
  - o resumo do slot passa a refletir a faixa correta, por exemplo `Mean 31-60`.
- Exportacao:
  - o `Export Compare` deixou de usar o `ImageExporter` do `pyqtgraph` para os plots comparativos;
  - a exportacao agora renderiza cada plot em `QImage` com tamanho fixo `1600x1200`;
  - isso garante aspect ratio `4:3` horizontal, mais previsivel para relatorios.

## Ajuste local da planilha de plots - 2026-03-08
- A planilha `config/config_incertezas_rev3.xlsx` tinha uma alteracao local pendente e agora ela deve ser tratada como mudanca consciente de configuracao.
- Alteracao identificada:
  - aba `Plots`;
  - linha do grafico `t_s_agua_vs_power_all.png`;
  - campo `y_max` alterado de `90` para `100`.
- Impacto:
  - o plot `Engine Coolant temperature vs Power (all fuels)` passa a usar teto visual de `100 °C` no eixo Y em vez de `90 °C`.
