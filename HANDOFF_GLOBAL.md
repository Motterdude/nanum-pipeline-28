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

## Consolidacao pipeline 29 para o dataset Wagner - 2026-03-20
- Objetivo:
  - consolidar no Git a rodada de evolucao do `pipeline29` focada nos combustiveis do Wagner, no novo fluxo de plots pela GUI, no calculo de airflow e nas emissoes especificas em `g/kWh`.
- Arquivos afetados:
  - `nanum_pipeline_29.py`
  - `pipeline29_config_backend.py`
  - `pipeline29_config_gui.py`
  - `config/pipeline29_text/mappings.toml`
  - `config/pipeline29_text/plots.toml`
  - `config/config_incertezas_rev3.xlsx`
  - `CHANGELOG.md`
  - `HANDOFF_GLOBAL.md`
- Escopo funcional consolidado:
  - parse e rotulagem de combustiveis expandidos para o Wagner:
    - `B100`
    - `D85B15`
    - `B40E60`
    - `B50E50`
    - `B90E10`
  - correcao do filtro/seletor de pontos para plots, evitando duplicacao de colunas por `Fuel_Label` repetido;
  - checkboxes na aba `Plots` para escolher diretamente:
    - com incerteza
    - sem incerteza
    - ambos
  - expansao do runner para gerar as duas variantes sem sobrescrever arquivos;
  - travamento de escala comum entre plots com e sem incerteza da mesma serie;
  - airflow refeito com prioridade operacional:
    - `MAF` valido por ponto primeiro;
    - fallback `fuel + lambda`;
    - fallback final com `lambda = 1.0` somente quando a lambda medida nao existir;
  - resumo final de terminal com:
    - metodo de airflow usado;
    - quantidades calculadas;
    - quantidades nao calculadas e motivo;
    - plots gerados/pulados e motivo;
  - incertezas de `T_E_COMP` e `T_S_COMP` herdando a mesma pilha instrumental de `T_S_AGUA` (termopar tipo K + `NI9213`);
  - `UMIDADE_ABS_g_m3` corrigida para usar `T_E_COMP` no lugar de `T_ADMISSAO`;
  - plot novo de `UMIDADE_mean_of_windows` na GUI, sem incerteza;
  - calculo de emissoes especificas em `g/kWh` para:
    - `CO2`
    - `CO`
    - `THC`
    - `NOx` como `NO`
    - `NOx` como `NO2`
  - calculo indireto de `H2O_wet_frac` e de `Exhaust_H2O_kg_h`, com colunas intermediarias no `lv_kpis_clean.xlsx` para debug;
  - plots novos no fluxo padrao para:
    - `CO2_g_kWh`
    - `CO_g_kWh`
    - `THC_g_kWh`
    - `NOx_as_NO_g_kWh`
    - `NOx_as_NO2_g_kWh`
    - `Exhaust_H2O_kg_h`
- Integracao de configuracao:
  - o caminho `--config-source excel` do `pipeline29` estava pulando a normalizacao do backend e por isso ignorava os plots novos;
  - isso foi corrigido ao fazer o loader Excel usar o mesmo bundle normalizado que a GUI/texto usam;
  - o backend agora injeta automaticamente os plots obrigatorios de emissoes e agua quando a config antiga ainda nao os tiver.
- Estado atual da `config_incertezas_rev3.xlsx` versionada:
  - a planilha sofreu somente sincronizacao dos paths de runtime na aba `Defaults`;
  - valores atuais:
    - `RAW_INPUT_DIR = C:\Users\SC61730\Downloads\raw_wagnao`
    - `OUT_DIR = C:\Users\SC61730\Downloads\out_wagnao`
  - nao houve inclusao manual adicional de regras/metadados nessa planilha nesta rodada.
- Validacao executada:
  - compilacao por `py_compile` de:
    - `nanum_pipeline_29.py`
    - `pipeline29_config_backend.py`
    - `pipeline29_config_gui.py`
  - execucoes completas do `pipeline29` em `raw_wagnao`, tanto via config textual quanto via `--config-source excel`;
  - confirmacao de geracao dos PNGs:
    - `co2_g_kwh_vs_power_all.png`
    - `co_g_kwh_vs_power_all.png`
    - `thc_g_kwh_vs_power_all.png`
    - `nox_as_no_g_kwh_vs_power_all.png`
    - `nox_as_no2_g_kwh_vs_power_all.png`
    - `exhaust_h2o_kg_h_vs_power_all.png`
  - confirmacao de que o terminal resume:
    - `Airflow`
    - calculos validos/faltantes
    - plots gerados/pulados
- Leitura operacional mais recente do Wagner:
  - `Airflow`: todos os `46` pontos processados sairam por `MAF`; nao houve `lambda` medida disponivel na MoTeC nessa rodada;
  - `THC`: todos os `46` pontos ficaram em faixa baixa de sinal e `30` ficaram negativos; o pipeline preserva calculo/plot e acusa `WARN` no terminal;
  - `Consumo_L_h` e `Custo_R_h` continuam faltando para:
    - `B100`
    - `B40E60`
    - `B50E50`
    - `B90E10`
  - motivo:
    - ainda faltam densidade/custo desses blends em `Fuel Properties` / `Defaults`.
- Sanity check de emissoes realizado nesta data:
  - `CO2_g_kWh` do Wagner caiu na faixa aproximada `735..931 g/kWh`;
  - a comparacao com estequiometria do carbono do combustivel e `BSFC` deixou os pontos entre cerca de `89%` e `103%` do valor teorico;
  - a agua no escape ficou na ordem de `1.11..1.54 kg H2O/kg fuel`, o que foi considerado plausivel para diesel/biodiesel/etanol quando se soma:
    - agua formada pelo H do combustivel
    - umidade do ar admitido
  - nesta checagem nao foi identificada correcao obrigatoria adicional de codigo.

## Snapshot congelado do pipeline 28 e abertura do pipeline 29 - 2026-03-13
- Objetivo:
  - congelar o estado atual do `nanum_pipeline_28.py` antes da migracao planejada de configuracao via Excel para GUI + texto;
  - abrir a proxima linha de evolucao em `nanum_pipeline_29.py`, mantendo rollback simples para o `28`.
- Arquivos afetados:
  - `nanum_pipeline_28.py`
  - `nanum_pipeline_29.py`
  - `config/config_incertezas_rev3.xlsx`
  - `CHANGELOG.md`
  - `README_EXECUCAO.md`
  - `HANDOFF_GLOBAL.md`
- Consolidacao funcional incluida neste snapshot:
  - incertezas de temperatura expandidas para:
    - `T_S_CIL_1..4`
    - `T_CARTER`
    - `T_AMBIENTE`
    - `T_RADIADOR`
    - `T_S_AGUA`
    - `T_WATERCOOLER`
    - `T_ADMISSAO`
    - `T_E_TURB`
    - `T_S_TURB`
  - derivacao com incerteza para:
    - `T_E_CIL_AVG`
    - `DT_ADMISSAO_TO_T_E_CIL_AVG_C`
  - seletor `NI9213_TC_MODE` na aba `Defaults`, mantendo `high_speed` como default e suportando troca por configuracao;
  - bypass de popup por ambiente:
    - `PIPELINE28_USE_DEFAULT_RUNTIME_DIRS=1` no `28`
    - `PIPELINE29_USE_DEFAULT_RUNTIME_DIRS=1` no `29`, com fallback para a variavel antiga;
  - incertezas de pressao aplicadas via Excel para:
    - `P_S_TURB_RAW`
    - `P_E_TURB_RAW`
    - `P_COLETOR_RAW`
    - `P_S_COMP_RAW`
  - criterio de pressao adotado:
    - limite informado do sensor `+/-2.93 kPa`;
    - entrada na aba `Instruments` como distribuicao retangular;
    - `uB = 2.93 / sqrt(3) = 1.6916 kPa`.
- Validacao local registrada:
  - compilacao de `nanum_pipeline_28.py` e `nanum_pipeline_29.py`;
  - `lv_kpis_clean.xlsx` com colunas `uA/uB/uc/U` preenchidas para os canais de temperatura acima e para os quatro canais de pressao;
  - plots de temperatura e pressao relevantes com `yerr_col` configurado.
- Decisao operacional a partir deste ponto:
  - `nanum_pipeline_28.py` passa a ser o rollback congelado deste snapshot;
  - novas mudancas estruturais devem partir de `nanum_pipeline_29.py`.

## Migracao inicial de configuracao para texto + GUI no pipeline 29 - 2026-03-13
- Objetivo:
  - retirar do `pipeline29` a dependencia operacional da planilha como fonte principal de configuracao;
  - preparar a base para uma GUI de configuracao equivalente ao Excel, mas versionavel e menos fragil.
- Arquitetura aplicada:
  - backend compartilhado em `pipeline29_config_backend.py`;
  - editor grafico em `pipeline29_config_gui.py`;
  - configuracao textual versionada em `config/pipeline29_text/`.
- Formato adotado:
  - TOML versionado para a configuracao base:
    - `defaults.toml`
    - `data_quality.toml`
    - `mappings.toml`
    - `instruments.toml`
    - `reporting_rounding.toml`
    - `plots.toml`
  - JSON local para presets/estado da GUI em `%LOCALAPPDATA%\nanum_pipeline_29\`.
- Fluxo atual do `pipeline29`:
  - `--config-source auto`:
    - usa `config/pipeline29_text/` se existir;
    - se nao existir, bootstrapa a partir de `config/config_incertezas_rev3.xlsx`.
  - `--config-source text`:
    - exige a pasta textual.
  - `--config-source excel`:
    - usa a planilha diretamente como fallback.
- GUI entregue nesta rodada:
  - abas para:
    - `Defaults`
    - `Data Quality`
    - `Mappings`
    - `Instruments`
    - `Reporting`
    - `Plots`
  - botoes de:
    - adicionar linha
    - duplicar selecionadas
    - remover selecionadas
    - recarregar texto
    - importar Excel -> texto
    - salvar config textual
    - validar
    - salvar preset
    - carregar preset
- GUI refinada depois na mesma data:
  - `Save` passou a sobrescrever a config textual atual;
  - `Save As` passou a gravar em outro diretorio de config;
  - o editor ganhou `Variable source` dedicado para montar catalogo de colunas a partir de `.xlsx/.csv`;
  - colunas de variavel em `Mappings` e `Plots` agora podem ser preenchidas via seletor pesquisavel com wildcard, acionado por duplo clique.
  - `Add row` de `Mappings`, `Instruments` e `Plots` passou a abrir helper vertical em dialog separado;
  - o helper de `Instruments` recebeu dropdown editavel de `key`, alimentado pelas `keys` atuais do `Mappings` ainda nao salvas.
  - a janela principal da GUI passou a abrir maximizada;
  - o helper de `Mappings` passou a sugerir `col_sd` automaticamente a partir de `col_mean`;
  - o helper de `Instruments` passou a:
    - explicar `acc_abs` como limite `+/-`;
    - preencher `acc_pct`, `digits`, `lsd` e `resolution` com `0` por default;
    - assumir `source = User input` por default;
    - mostrar descricao dinamica para o `source` selecionado;
  - o helper de `Plots` passou a abrir com defaults de setup rapido (`enabled = 1`, `plot_type = all_fuels_yx`, `x = 0..55 passo 5`) e a gerar `filename/title` automaticamente.
- Fluxo de execucao refinado no `nanum_pipeline_29.py`:
  - no run normal, pergunta se a GUI deve ser aberta antes do processamento;
  - para automacao, pode pular essa pergunta com:
    - `--skip-config-gui-prompt`
    - `PIPELINE29_SKIP_CONFIG_GUI_PROMPT=1`
- Plot com/sem incerteza:
  - nova chave `show_uncertainty` por linha em `plots.toml`;
  - valores suportados:
    - `auto`
    - `on`
    - `off`
  - `off` suprime as barras de erro mesmo que `yerr_col` exista;
  - `auto/on` permitem usar `yerr_col` explicito ou fallback automatico por `U_*`.
- Validacao feita:
  - compilacao de:
    - `nanum_pipeline_29.py`
    - `pipeline29_config_backend.py`
    - `pipeline29_config_gui.py`
  - bootstrap real da `rev3` para `config/pipeline29_text/`;
  - carregamento do bundle textual pelo proprio `nanum_pipeline_29.py`;
  - smoke test da GUI em `offscreen` dentro da `.venv` do repo.

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

## Como puxar tudo no PC do trabalho - 2026-03-08
- Repositorio remoto oficial:
  - `https://github.com/Motterdude/nanum-pipeline-28`
- Fluxo recomendado daqui para frente:
  - usar Git como fonte principal do codigo;
  - deixar o Drive apenas para dados pesados, backups ou transferencia de arquivos nao versionados.
- Se o PC do trabalho ainda nao tiver o repo clonado:
  - abrir um terminal na pasta onde o projeto deve ficar;
  - rodar:
    - `git clone https://github.com/Motterdude/nanum-pipeline-28`
    - `cd nanum-pipeline-28`
- Se o PC do trabalho ja tiver a pasta clonada:
  - abrir um terminal dentro da pasta do repo;
  - conferir se ha mudancas locais com `git status`;
  - se estiver limpo, rodar:
    - `git checkout main`
    - `git pull --ff-only origin main`
- Se `git status` no PC do trabalho mostrar mudancas locais:
  - nao dar `pull` por cima sem revisar;
  - primeiro salvar essas mudancas com commit local ou `git stash`;
  - depois rodar `git pull --ff-only origin main`.
- Commits relevantes desta rodada:
  - `bcf5840` `add multi-file comparison to fast viewer`
  - `7e4dd5f` `raise coolant plot y max to 100`
  - `9177a19` `fix block mean selection and 4x3 compare export`
- Arquivos principais adicionados/ajustados nesta rodada:
  - `standalone_kibox_cycle_viewer_fast.py`
  - `requirements_gui_viewer.txt`
  - `config/config_incertezas_rev3.xlsx`
  - `HANDOFF_GLOBAL.md`

## Ambiente e execucao versionados - 2026-03-08
- Para reduzir dependencia de ambiente solto e configuracao manual fora do repo, foram adicionados artefatos de execucao versionados:
  - `requirements_pipeline.txt`;
  - `requirements_full.txt`;
  - `setup_env.ps1`;
  - `README_EXECUCAO.md`.
- Objetivo:
  - permitir criar uma `.venv` local do repo em qualquer PC;
  - registrar no Git quais pacotes sao necessarios para o `pipeline28` e para o viewer Qt rapido;
  - manter no proprio repo a instrucao operacional de setup e execucao.
- Dependencias de referencia do pipeline:
  - `pandas 3.0.1`;
  - `numpy 2.4.2`;
  - `matplotlib 3.10.8`;
  - `openpyxl 3.1.5`;
  - `python-calamine 0.6.2`.
- Dependencias adicionais do viewer rapido:
  - `PySide6 6.10.2`;
  - `pyqtgraph 0.14.0`.
- `README_EXECUCAO.md` centraliza:
  - quais arquivos precisam existir para rodar;
  - o que fica fora do Git;
  - como criar o ambiente;
  - como rodar pipeline e viewers;
  - regras para evitar conflito entre PCs.
- `setup_env.ps1` cria `.venv` local e instala:
  - `requirements_pipeline.txt` por padrao;
  - `requirements_full.txt` quando usado com `-WithGui`.

## Correcao de RAW_INPUT_DIR/OUT_DIR no PC de trabalho - 2026-03-09
- Erro reportado no PC de trabalho:
  - `FileNotFoundError: Nao encontrei o diretorio configurado em RAW_INPUT_DIR: D:\Drive\Faculdade\PUC\Mestrado\Dados_Ensaios\Processamento_Pyton\raw`.
- Causa:
  - a aba `Defaults` em `config/config_incertezas_rev3.xlsx` ainda estava com caminhos absolutos do notebook de casa (`D:\Drive\...`), que nao existem no PC atual.
- Correcao de configuracao aplicada (aba `Defaults`):
  - `RAW_INPUT_DIR = C:\Users\SC61730\Downloads\_tmp_nanum_pipeline_28_remote_20260309\raw\PROCESSAR`
  - `OUT_DIR = C:\Users\SC61730\Downloads\_tmp_nanum_pipeline_28_remote_20260309\out`
- Hardening de codigo aplicado em `nanum_pipeline_28.py`:
  - `apply_runtime_path_overrides()` agora faz fallback automatico para `BASE_DIR/raw/PROCESSAR` quando `RAW_INPUT_DIR` configurado nao existe no PC, emitindo `[WARN]`.
  - para `OUT_DIR`, quando o caminho configurado nao pode ser preparado, o pipeline faz fallback para `BASE_DIR/out`, tambem com `[WARN]`.
  - objetivo: evitar quebra imediata ao alternar entre PCs com caminhos absolutos diferentes.

## Workflow unico do executavel KIBOX - 2026-03-09
- Problema reportado:
  - o `standalone_kibox_cycle_viewer_fast.py` podia falhar na abertura quando o `--input` default apontava para um caminho antigo de outro PC.
- Correcao aplicada no viewer rapido:
  - o viewer agora e auto-contido e nao depende mais de `standalone_kibox_cycle_plots.py` para constantes/parse;
  - quando o `--input` default nao existe (ou nao e arquivo), ao abrir normalmente ele mostra `QFileDialog` para selecionar o CSV na hora;
  - se o usuario cancelar a selecao, a execucao e encerrada com mensagem clara.
- Limpeza do workflow:
  - removidos do repositorio os utilitarios antigos:
    - `standalone_kibox_cycle_plots.py`;
    - `standalone_kibox_cycle_viewer.py`.
  - o utilitario oficial unico para KIBOX passa a ser:
    - `standalone_kibox_cycle_viewer_fast.py`.
- Documentacao atualizada:
  - `README_EXECUCAO.md` agora lista apenas o viewer rapido no fluxo operacional.

## Ajuste visual no viewer rapido KIBOX (fundo preto) - 2026-03-09
- Pedido: melhorar contraste de `pressure trace` (`PCYL_1`) e `heat release` (`Q_1`) no tema com fundo preto.
- Correcao aplicada em `standalone_kibox_cycle_viewer_fast.py`:
  - paleta principal trocada para cores vividas/neon:
    - `PCYL_1` selecionado: ciano neon;
    - `Q_1` selecionado: laranja vivo;
    - `Block mean`: amarelo vivo tracejado.
  - espessura das curvas aumentada para facilitar leitura:
    - viewer principal (`PCYL_1`/`Q_1`) e compare (`slots`) com linhas mais espessas.
  - cores de `PMAX` tambem reforcadas para manter consistencia visual no tema escuro.
- Resultado esperado:
  - melhor separacao visual das curvas sobre fundo preto, tanto na aba `Viewer` quanto na aba `Compare`.

## Export claro com linhas/texto/grid escuros no viewer rapido - 2026-03-09
- Pedido: manter o visual em tempo real no tema escuro, mas exportar os graficos com estilo de relatorio (fundo claro + elementos escuros) e sem sobras laterais em branco.
- Correcao aplicada em `standalone_kibox_cycle_viewer_fast.py`:
  - `_export_plot_item_png()` agora aplica um tema temporario de export:
    - fundo branco;
    - curvas em paleta escura;
    - eixos, labels, titulos, legenda e grid em tons escuros.
  - apos salvar o PNG, o estilo original do viewer e restaurado automaticamente (sem alterar o tema ao vivo).
  - render de export alterado para `IgnoreAspectRatio` para preencher completamente o canvas fixo de export e remover barras/espacos laterais.
- Resultado esperado:
  - PNGs exportados mais legiveis para documento/apresentacao, mantendo a experiencia interativa escura no app.

## Reversao de estilo de export para fidelidade com o real-time - 2026-03-09
- Problema reportado apos o ajuste anterior:
  - export ficou visualmente deformado e diferente do que aparece no viewer.
- Correcao aplicada:
  - `_export_plot_item_png()` voltou a exportar o plot sem trocar tema, mantendo exatamente o estilo do real-time (cores, textos, grid e fundo).
  - o tamanho final de export agora preserva a proporcao nativa do plot antes de renderizar, evitando estiramento/deformacao.
  - o preenchimento do canvas usa o mesmo fundo do `ViewBox` ativo para nao surgir laterais brancas.
- Resultado esperado:
  - export fiel ao que esta na tela, sem distorcao geometrica e sem faixas brancas laterais.

## Sincronizacao de eixo X na aba Viewer - 2026-03-09
- Pedido: quando houver mudanca de eixo X via GUI na aba `Viewer`, manter os plots alinhados.
- Correcao aplicada em `standalone_kibox_cycle_viewer_fast.py`:
  - adicionado sincronizador de `sigXRangeChanged` entre os dois plots de crank-angle (`PCYL_1` e `Q_1`);
  - ao dar pan/zoom em qualquer um deles, o mesmo range X e aplicado no outro automaticamente;
  - incluido lock interno para evitar loop de eventos durante a sincronizacao.
- Resultado esperado:
  - `PCYL_1` e `Q_1` permanecem alinhados no eixo X durante ajustes interativos no viewer.

## Caminhos de runtime estritos via Excel (pipeline28) - 2026-03-09
- Problema reportado:
  - o pipeline nao estava respeitando o caminho de input configurado no Excel e acabava buscando em diretorio diferente.
- Causa raiz:
  - a aba `Defaults` ainda estava salva com caminhos antigos (`..._tmp_nanum_pipeline_28_remote...`);
  - o codigo tinha fallback automatico para diretorios default locais quando `RAW_INPUT_DIR/OUT_DIR` falhavam, o que mascarava configuracao incorreta.
- Correcao aplicada em `nanum_pipeline_28.py`:
  - `apply_runtime_path_overrides()` passou a operar em modo estrito:
    - usa exatamente `RAW_INPUT_DIR` e `OUT_DIR` vindos do Excel quando preenchidos;
    - remove fallback silencioso para defaults;
    - se `RAW_INPUT_DIR` nao existir, falha com erro explicito;
    - se `OUT_DIR` nao puder ser criado/acessado, falha com erro explicito.
  - adicionados logs de diagnostico com os caminhos efetivos lidos do Excel:
    - `[INFO] RAW_INPUT_DIR (Excel): ...`
    - `[INFO] OUT_DIR (Excel): ...`
- Correcao aplicada no `config/config_incertezas_rev3.xlsx` (aba `Defaults`):
  - `RAW_INPUT_DIR = C:\Users\SC61730\Downloads\raw_mestrado`
  - `OUT_DIR = C:\Users\SC61730\Downloads\out_mestrado`
- Validacao executada:
  - run completo do `nanum_pipeline_28.py` concluido com sucesso;
  - log confirmou uso exato dos caminhos acima;
  - saidas geradas em `C:\Users\SC61730\Downloads\out_mestrado` (`lv_time_diagnostics.xlsx`, `lv_diagnostics_summay.xlsx`, `lv_kpis_clean.xlsx` e plots).

## Nova conta de ignition delay (MoTeC x KIBOX AI05) - 2026-03-09
- Pedido:
  - calcular ignition delay como delta absoluto entre `Motec Ignition advance` e `KIBOX_AI05`, considerando convencao de sinal:
    - MoTeC positivo = `BTDC`;
    - KIBOX positivo = `ATDC`.
- Regra implementada:
  - converter para mesmo referencial (`ATDC`) e calcular:
    - `Ignition_Delay_abs_degCA = abs(KIBOX_AI05_1 + Motec_Ignition Timing_mean_of_windows)`.
- Implementacao:
  - coluna derivada criada no `build_final_table()` de `nanum_pipeline_28.py`;
  - nome final da coluna no output: `Ignition_Delay_abs_degCA`.
- Plot novo na aba `Plots` de `config/config_incertezas_rev3.xlsx`:
  - `filename = ignition_delay_vs_upd_power_all.png`;
  - `plot_type = all_fuels_yx`;
  - `x_col = UPD_Power_Bin_kW` (potencia UPD medida);
  - `y_col = Ignition_Delay_abs_degCA`;
  - `yerr_col = off`.
- Comportamento para dados faltantes (confirmado):
  - o workflow de `all_fuels_*` ja remove `NaN` por serie (`dropna`) e pula apenas a curva/combustivel sem dados;
  - o plot inteiro so e pulado quando nenhuma serie tem dados validos.
- Validacao executada:
  - run completo do pipeline concluido em `C:\Users\SC61730\Downloads\raw_mestrado`;
  - `lv_kpis_clean.xlsx` contem `Ignition_Delay_abs_degCA` com `20` linhas validas (de `30`);
  - plot gerado com sucesso em:
    - `C:\Users\SC61730\Downloads\out_mestrado\plots\raw\ignition_delay_vs_upd_power_all.png`.

## Ajuste de escala do plot ignition delay - 2026-03-09
- Pedido:
  - eixo Y com marcacao de `0.5 em 0.5 deg`;
  - eixo X no mesmo padrao visual dos plots KIBOX.
- Ajuste aplicado na aba `Plots` de `config/config_incertezas_rev3.xlsx` para `ignition_delay_vs_upd_power_all.png`:
  - `x_min = 0`;
  - `x_max = 55`;
  - `x_step = 5`;
  - `y_step = 0.5`.
- Mantido:
  - `x_col = UPD_Power_Bin_kW` (potencia UPD medida, conforme pedido anterior).
- Validacao:
  - run completo do pipeline concluido;
  - plot regenerado em `C:\Users\SC61730\Downloads\out_mestrado\plots\raw\ignition_delay_vs_upd_power_all.png`;
  - configuracao confirmada no Excel com `x=0..55 step 5` e `y_step=0.5`.

## Consumo equivalente de etanol (E94H6 x E75H25 x E65H35) - 2026-03-09
- Pedido:
  - usar a conta de consumo equivalente de etanol para compensar o efeito da agua nas misturas hidratadas;
  - sobrepor `E94H6` (consumo lido na base equivalente), `E75H25` e `E65H35`;
  - criar grafico adicional de razao percentual para validar a coerencia da conta.
- Base de calculo adotada (ja existente no pipeline):
  - `Fuel_E94H6_eq_kg_h = Fuel_EtOH_pure_kg_h / 0.94`;
  - para `E94H6` isso preserva o consumo lido (equivalente ao valor medido).
- Implementacao em `nanum_pipeline_28.py`:
  - nova rotina `_plot_ethanol_equivalent_consumption_overlay(...)`;
  - nova rotina `_plot_ethanol_equivalent_ratio(...)`;
  - selecao de blends por `EtOH_pct/H2O_pct` com tolerancia de `0.6` ponto percentual;
  - integracao no fluxo final de plots por `source_folder`.
- Graficos gerados:
  - `consumo_equiv_etanol_vs_upd_power_overlay.png`:
    - sobreposicao de `Fuel_E94H6_eq_kg_h` vs `UPD_Power_Bin_kW` para `E94H6`, `E75H25`, `E65H35`;
  - `consumo_equiv_etanol_ratio_pct_vs_upd_power.png`:
    - `100 * (E94H6 / E75H25)` e `100 * (E94H6 / E65H35)` com linha de referencia em `100%`.
- Regra para dados faltantes (confirmada):
  - se faltar dado de um blend especifico, apenas aquela curva e pulada;
  - o grafico so e pulado quando nenhuma curva valida existe.
- Validacao:
  - run completo executado com sucesso em:
    - `RAW_INPUT_DIR = C:\Users\SC61730\Downloads\raw_mestrado`;
    - `OUT_DIR = C:\Users\SC61730\Downloads\out_mestrado`;
  - os dois PNGs novos foram gerados em:
    - `C:\Users\SC61730\Downloads\out_mestrado\plots\raw\`.

## n_th por fluxo equivalente E94H6 + comparacao 6 curvas - 2026-03-09
- Pedido:
  - manter `n_th` original (com LHV da mistura);
  - adicionar `n_th` com base em energia disponivel por vazao equivalente E94H6;
  - gerar:
    - grafico dedicado de `n_th_E94H6_eq_flow`;
    - grafico de comparacao com 6 curvas (3 misturas com LHV original + 3 misturas com equivalente E94H6).
- Implementacao em `nanum_pipeline_28.py`:
  - novo lookup robusto de LHV para blend de referencia:
    - `_lookup_lhv_for_blend(...)`;
    - busca `EtOH_pct/H2O_pct` com tolerancia de `0.6`.
  - no `build_final_table()`:
    - nova coluna de referencia: `LHV_E94H6_kJ_kg`;
    - nova energia quimica por LHV original: `Qdot_fuel_LHV_mix_kW`;
    - nova energia quimica por equivalente E94H6: `Qdot_fuel_E94H6_eq_kW`;
    - nova eficiencia: `n_th_E94H6_eq_flow`;
    - nova eficiencia em percentual: `n_th_E94H6_eq_flow_pct`.
  - regra aplicada:
    - `n_th_E94H6_eq_flow = PkW / ((Fuel_E94H6_eq_kg_h/3600) * LHV_E94H6_kJ_kg)`;
    - invalida quando potencia, vazao equivalente ou LHV de referencia forem nao positivos.
- Novos plots automaticos no fluxo final:
  - `nth_e94h6_eq_flow_vs_upd_power_all.png`:
    - apenas `E94H6`, `E75H25`, `E65H35` para `n_th_E94H6_eq_flow_pct`;
  - `nth_lhv_vs_e94h6_eq_flow_6curves.png`:
    - 6 curvas:
      - `E94H6 | n_th_lhv` e `E94H6 | n_th_E94H6_eq_flow`;
      - `E75H25 | n_th_lhv` e `E75H25 | n_th_E94H6_eq_flow`;
      - `E65H35 | n_th_lhv` e `E65H35 | n_th_E94H6_eq_flow`.
- Validacao:
  - run completo concluido com:
    - `RAW_INPUT_DIR = C:\Users\SC61730\Downloads\raw_mestrado`;
    - `OUT_DIR = C:\Users\SC61730\Downloads\out_mestrado`;
  - os dois PNGs novos foram gerados em:
    - `C:\Users\SC61730\Downloads\out_mestrado\plots\raw\`;
  - checagem em `lv_kpis_clean.xlsx` confirmou:
    - `n_th_E94H6_eq_flow_pct` presente;
    - para `E94H6`, `n_th_pct == n_th_E94H6_eq_flow_pct` (como esperado).

## Delta percentual no ratio de consumo equivalente - 2026-03-09
- Pedido:
  - no grafico `consumo_equiv_etanol_ratio_pct_vs_upd_power`, exibir delta percentual relativo a `100%`;
  - exemplo de regra: `102%` deve aparecer como `+2%`.
- Ajuste aplicado em `nanum_pipeline_28.py`:
  - dentro de `_plot_ethanol_equivalent_ratio(...)`:
    - mantem conta base `ratio_pct = 100 * (E94H6 / blend)` apenas como intermediaria;
    - novo valor plotado: `delta_pct = ratio_pct - 100`.
  - linha de referencia alterada:
    - de `100%` para `0% (ref = 100%)`.
  - textos do grafico atualizados para refletir `delta percentual`.
- Resultado visual:
  - `+2` no eixo Y significa `E94H6` consome `2%` a mais (equivalente) que a mistura comparada naquele ponto;
  - `-2` significa `2%` a menos.
- Validacao:
  - run completo do pipeline concluido com sucesso;
  - plot atualizado em:
    - `C:\Users\SC61730\Downloads\out_mestrado\plots\raw\consumo_equiv_etanol_ratio_pct_vs_upd_power.png`.

## Transicao sem esforco (trabalho -> casa) - 2026-03-09
- Objetivo:
  - retomar o desenvolvimento em casa exatamente do estado atual do laboratorio, sem perder configuracao, codigo ou contexto.
- Estado de referencia publicado:
  - branch: `main`;
  - commit de checkpoint de transicao: `8113457`;
  - tag de checkpoint: `checkpoint-2026-03-09-lab-sync`;
  - remoto: `https://github.com/Motterdude/nanum-pipeline-28`.
- Passo a passo no PC de casa (primeira vez):
  - `git clone https://github.com/Motterdude/nanum-pipeline-28`
  - `cd nanum-pipeline-28\Processamentos`
  - `git fetch --all --tags`
  - `git checkout main`
  - `git pull --ff-only origin main`
- Passo a passo no PC de casa (repo ja existente):
  - `cd <repo>\Processamentos`
  - `git status` (garantir limpo ou salvar mudancas locais)
  - `git fetch --all --tags`
  - `git checkout main`
  - `git pull --ff-only origin main`
- Opcional para reproduzir exatamente o estado do laboratorio:
  - `git checkout checkpoint-2026-03-09-lab-sync`
  - opcional: `git switch -c continue-from-lab`
- Ambiente Python (no repo):
  - pipeline + viewer:
    - `powershell -ExecutionPolicy Bypass -File .\setup_env.ps1 -WithGui`
  - pipeline apenas:
    - `powershell -ExecutionPolicy Bypass -File .\setup_env.ps1`
- Configuracao obrigatoria antes de rodar:
  - abrir `config/config_incertezas_rev3.xlsx` (aba `Defaults`);
  - ajustar para caminhos locais do PC de casa:
    - `RAW_INPUT_DIR`
    - `OUT_DIR`
  - observacao: se `RAW_INPUT_DIR` estiver preenchido e nao existir, o pipeline para com erro explicito (sem fallback silencioso).
- Execucao:
  - `& ".\.venv\Scripts\python.exe" .\nanum_pipeline_28.py`
- Confirmacao rapida no log:
  - validar estas linhas no inicio:
    - `[INFO] RAW_INPUT_DIR (Excel): ...`
    - `[INFO] OUT_DIR (Excel): ...`
  - isso garante que a execucao esta lendo exatamente os caminhos esperados no PC de casa.
- Regra de retorno para o laboratorio (mesmo fluxo):
  - antes de sair de casa:
    - `git add ...`
    - `git commit -m "..."`
    - `git push origin main`
  - registrar no `HANDOFF_GLOBAL.md` o que mudou (codigo, planilha e validacao).

## Execucao com RAW_NANUM + robustez de ignition delay sem Motec/KIBOX - 2026-03-10
- Pedido operacional:
  - rodar usando `RAW_INPUT_DIR = C:\Users\SC61730\Downloads\raw_NANUM`.
- Ajuste aplicado na configuracao:
  - `config/config_incertezas_rev3.xlsx` (aba `Defaults`):
    - `RAW_INPUT_DIR` atualizado para `C:\Users\SC61730\Downloads\raw_NANUM`;
    - `OUT_DIR` mantido em `C:\Users\SC61730\Downloads\out_mestrado`.
- Problema identificado durante a primeira execucao:
  - dataset `raw_NANUM` nao possui colunas Motec/KIBOX necessarias para ignition delay;
  - o trecho de calculo usava fallback escalar e falhava com:
    - `AttributeError: 'numpy.float64' object has no attribute 'abs'`.
- Correcao aplicada em `nanum_pipeline_28.py`:
  - no calculo de `Ignition_Delay_abs_degCA`, fallback passou a ser `pd.Series(..., index=df.index)` para ambas colunas:
    - `Motec_Ignition Timing_mean_of_windows`;
    - `KIBOX_AI05_1`.
  - com isso, quando a coluna nao existe, o resultado fica `NaN` (comportamento esperado) e o pipeline segue.
- Validacao:
  - run completo concluido com sucesso;
  - log confirmou:
    - `[INFO] RAW_INPUT_DIR (Excel): C:\Users\SC61730\Downloads\raw_NANUM`
    - `[INFO] Entrada LabVIEW/Kibox: C:\Users\SC61730\Downloads\raw_NANUM`
  - saidas geradas em `C:\Users\SC61730\Downloads\out_mestrado`.

## Compare iteracoes BL vs ADTV (diesel) com incertezas - 2026-03-10
- Pedido:
  - comparar consumo diesel entre `baseline_1` e `aditivado_1`;
  - gerar comparacao para:
    - media de `subida + descida`;
    - apenas `subida` (`subindo_baseline_1` vs `subindo_aditivado_1`);
    - apenas `descida` (`descendo_baseline_1` vs `descendo_aditivado_1`);
  - entregar consumo absoluto e razao percentual com sinal:
    - negativo = economia no aditivado;
    - positivo = piora no aditivado.
- Implementacao em `nanum_pipeline_28.py`:
  - nova cadeia de funcoes dedicada:
    - identificacao de campanha BL/ADTV por `BaseName`;
    - filtro diesel (`DIES_pct/BIOD_pct > 0`);
    - agregacao por carga com incertezas;
    - media entre `subida` e `descida` por campanha;
    - plot absoluto com barras de erro;
    - plot de delta percentual com barras de erro propagadas.
  - incertezas expressas nos graficos:
    - absoluto: `U = 2*sqrt(uA^2 + uB^2)` (`uA` desvio padrao, `uB` balanca);
    - delta percentual:
      - `delta_pct = 100 * (cons_adtv/cons_bl - 1)`;
      - propagacao por incerteza relativa de BL e ADTV.
  - integracao no fluxo:
    - chamada automatica em `main()` via `_plot_compare_iteracoes_bl_vs_adtv(out, root_plot_dir=PLOTS_DIR)`.
- Arquivos gerados (pasta `plots/compare_iteracoes_bl_vs_adtv`):
  - `compare_iteracoes_bl_vs_adtv_consumo_medio_subida_descida.png`
  - `compare_iteracoes_bl_vs_adtv_razao_delta_pct_medio_subida_descida.png`
  - `compare_iteracoes_bl_vs_adtv_consumo_subida.png`
  - `compare_iteracoes_bl_vs_adtv_razao_delta_pct_subida.png`
  - `compare_iteracoes_bl_vs_adtv_consumo_descida.png`
  - `compare_iteracoes_bl_vs_adtv_razao_delta_pct_descida.png`
- Observacao operacional:
  - durante a execucao, `out_mestrado` estava com arquivo Excel aberto e bloqueando limpeza inicial;
  - para validar sem interromper, o run foi feito em `OUT_DIR` temporario:
    - `C:\Users\SC61730\Downloads\out_mestrado_tmp_bl_adtv_20260310_135920`;
  - ao final, os 6 PNGs novos foram copiados para:
    - `C:\Users\SC61730\Downloads\out_mestrado\plots\compare_iteracoes_bl_vs_adtv`.

## Excel detalhado da propagacao da incerteza no delta BL vs ADTV - 2026-03-10
- Pedido:
  - exportar planilha detalhada dos consumos BL/ADTV;
  - quebrar passo a passo a incerteza da razao para inspecionar por que os deltas parecem pouco conclusivos.
- Implementacao:
  - adicionado builder de tabela detalhada:
    - `_build_bl_adtv_delta_table(...)`;
  - adicionado export:
    - `_export_compare_iteracoes_bl_adtv_excel(...)`;
  - integracao na rotina principal dos compares BL/ADTV.
- Arquivo gerado:
  - `compare_iteracoes_bl_vs_adtv_consumo_incertezas.xlsx`
  - pasta: `C:\Users\SC61730\Downloads\out_mestrado\plots\compare_iteracoes_bl_vs_adtv`
- Conteudo principal da planilha (por carga e comparacao `subida`, `descida`, `media_subida_descida`):
  - consumos e incertezas de cada lado:
    - `cons_bl_kg_h`, `uA_bl_kg_h`, `uB_bl_kg_h`, `uc_bl_kg_h`, `U_bl_kg_h`;
    - `cons_adtv_kg_h`, `uA_adtv_kg_h`, `uB_adtv_kg_h`, `uc_adtv_kg_h`, `U_adtv_kg_h`.
  - razao e delta:
    - `ratio_adtv_over_bl`, `delta_pct`, `delta_abs_kg_h`.
  - derivadas da propagacao:
    - `d_delta_d_cons_adtv_pct_per_kgh`;
    - `d_delta_d_cons_bl_pct_per_kgh`.
  - contribuicoes por componente:
    - `uA_contrib_from_adtv_pct`, `uA_contrib_from_bl_pct`, `uA_delta_pct`;
    - `uB_contrib_from_adtv_pct`, `uB_contrib_from_bl_pct`, `uB_delta_pct`.
  - combinado final:
    - `uc_delta_pct`, `U_delta_pct`;
    - checagem equivalente: `uc_delta_pct_from_uc_direct`, `U_delta_pct_from_uc_direct`;
    - leitura de impacto: `delta_over_U`, `significancia_95pct`, `interpretacao`.
- Regra de sinal mantida:
  - `delta_pct < 0`: economia no aditivado;
  - `delta_pct > 0`: piora no aditivado.

## Dispersao baseline subida vs descida (consumo relativo com incerteza) - 2026-03-10
- Pedido:
  - adicionar comparacao dedicada entre `baseline subida` e `baseline descida` para avaliar dispersao;
  - plotar consumo relativo com incerteza.
- Implementacao:
  - reaproveitada a mesma propagacao de incerteza de delta percentual;
  - nova comparacao adicionada ao workflow BL vs ADTV:
    - referencia: `baseline_subida`;
    - comparado: `baseline_descida`;
    - formula: `delta_pct = 100 * (cons_descida / cons_subida - 1)`.
- Novos outputs em `compare_iteracoes_bl_vs_adtv`:
  - `compare_iteracoes_bl_vs_adtv_baseline_subida_vs_descida_consumo_abs.png`
  - `compare_iteracoes_bl_vs_adtv_baseline_subida_vs_descida_razao_delta_pct.png`
- Excel detalhado atualizado:
  - `compare_iteracoes_bl_vs_adtv_consumo_incertezas.xlsx` agora inclui:
    - `Comparacao = baseline_subida_vs_descida`;
    - `interpretacao` especifica:
      - `descida_menor_que_subida`
      - `descida_maior_que_subida`.
- Leitura do novo delta:
  - `delta_pct < 0`: descida consumiu menos que subida;
  - `delta_pct > 0`: descida consumiu mais que subida.

## Wrapper OpenToCSV para KiBox `.open -> .csv` - 2026-03-10
- Contexto:
  - o notebook do laboratorio tem `KiBoxCockpit 3.2.5` instalado;
  - junto com ele existe o conversor:
    - `C:\Program Files (x86)\Kistler\CSVExportSeriell\OpenToCSV.exe`.
- Implementacao:
  - criado o utilitario novo:
    - `kibox_open_to_csv.py`;
  - ele pode rodar standalone pela CLI ou ser importado em Python por outro script, inclusive pelo `pipeline28` no futuro;
  - o wrapper aceita arquivo `.open` unico ou diretorio com varios `.open`;
  - quando a entrada e um diretorio, ele varre os `.open` recursivamente e converte um por um;
  - quando a entrada e um arquivo, ele isola o `.open` em pasta temporaria e chama o `OpenToCSV.exe` so para aquele arquivo.
- Padrao operacional adotado para compatibilidade com o pipeline:
  - `type=res`
  - `sep=tab`
  - `cno`
  - isso replica o estilo dos CSVs `_i.csv` ja conhecidos pelo `pipeline28`:
    - uma linha por ciclo;
    - primeira coluna com `Cycle number`;
    - separador tab.
- Modos de nome:
  - `source`:
    - mesmo stem do `.open`, trocando para `.csv`;
  - `pipeline`:
    - stem + `_i.csv`;
  - `tool`:
    - mantem o sufixo natural do `OpenToCSV` (`_res`, `_sig`, `_tim`);
  - `--output-name`:
    - permite forcar um nome final especifico, util quando o `.open` nao carrega composicao no nome e voce quer casar com um `.xlsx` ou com o naming do pipeline.
- Uso basico:
  - `& ".\.venv\Scripts\python.exe" .\kibox_open_to_csv.py "C:\caminho\arquivo.open" --type res --separator tab --name-mode pipeline`
- GUI Windows:
  - o mesmo `kibox_open_to_csv.py` agora tambem abre uma janela grafica com:
    - selecao multipla de arquivos `.open`;
    - selecao de diretorio de saida;
    - bootstrap do `OpenToCSV.exe` na primeira abertura:
      - se o executavel nao for encontrado automaticamente, a GUI pede para localizar o arquivo uma vez;
      - o caminho fica salvo em `%LOCALAPPDATA%\nanum_pipeline_28\kibox_open_to_csv_settings.json`;
      - se o notebook mudar ou a instalacao estiver em outra pasta, a GUI pede o novo caminho sem derrubar a execucao;
    - log em tempo real da execucao do `OpenToCSV`;
    - barra de progresso por arquivo e indicacao do arquivo atual;
    - naming forcado para `nome_original_i.csv`, voltado ao padrao de leitura do `pipeline28`.
  - a GUI tambem ganhou customizacao de nome:
    - campo para inserir texto adicional;
    - dropdown dinamico de ponto de insercao, baseado no nome do arquivo selecionado na lista;
    - o dropdown mostra o proprio nome amostra com o texto ja colocado na posicao escolhida;
    - exemplos de opcao visual:
      - `NANUM_xxxx_17,5KW-2026-03-06--20-17-31-041.open`
      - `NANUM_17,5KW_xxxx_-2026-03-06--20-17-31-041.open`
    - a conversao final continua gerando CSV no padrao do pipeline:
      - `..._i.csv`.
  - comando:
    - `& ".\.venv\Scripts\python.exe" .\kibox_open_to_csv.py --gui`

## Pipeline28 com popup para RAW_INPUT_DIR e OUT_DIR - 2026-03-10
- Motivo:
  - o fluxo via aba `Defaults` do Excel continuava causando erro de caminho errado entre PCs e entre diretorios de dados diferentes;
  - o pedido foi tirar a escolha operacional de `RAW_INPUT_DIR` e `OUT_DIR` do Excel e levar isso para popup Windows em toda execucao.
- Implementacao no `nanum_pipeline_28.py`:
  - a cada execucao, antes de limpar `OUT_DIR` e antes de ler os arquivos de ensaio, abre uma janela para escolher:
    - diretorio de entrada do pipeline;
    - diretorio de saida.
  - robustez do seletor:
    - primeiro tenta o seletor nativo de pastas do Windows via PowerShell/.NET;
    - se isso falhar, tenta popup Tkinter;
    - se a GUI falhar mesmo assim, cai para prompt manual no terminal.
  - a ultima selecao fica salva localmente em:
    - `%LOCALAPPDATA%\nanum_pipeline_28\pipeline28_runtime_paths.json`
  - na execucao seguinte, o popup volta preenchido com esses ultimos caminhos.
- Relacao com o Excel:
  - o restante do `config/config_incertezas_rev3.xlsx` continua sendo carregado normalmente;
  - apenas `RAW_INPUT_DIR` e `OUT_DIR` sao sincronizados de volta na aba `Defaults`;
  - nenhuma outra linha/aba da planilha e alterada por esse fluxo.
- Log esperado:
  - `[INFO] RAW_INPUT_DIR (GUI): ...`
  - `[INFO] OUT_DIR (GUI): ...`
  - `[INFO] Ultima selecao salva em: ...`
  - `[INFO] Aba Defaults sincronizada apenas para RAW_INPUT_DIR/OUT_DIR em: ...`
- Impacto operacional:
  - nao e mais necessario abrir o Excel para trocar pasta de entrada/saida entre notebook de casa, notebook do trabalho ou novas pastas de dados;
  - ainda fica registrado no Excel quais foram os ultimos caminhos usados, mas o valor operacional passa a ser o escolhido no popup daquela execucao.

## Correcao do merge KIBOX para diesel em `raw_NANUM` - 2026-03-10
- Sintoma observado:
  - `Subindo_baseline_2` tinha arquivos de combustao KIBOX no input (`*_i.csv`), mas o `out` nao gerava plots `kibox_*`;
  - ao mesmo tempo, a campanha podia herdar combustao de outra serie com mesma carga/composicao.
- Causas encontradas:
  - o parser de composicao aceitava `D85B15`, mas nao aceitava nomes invertidos como `B15D85`;
  - `kibox_aggregate()` descartava diesel porque exigia `EtOH_pct/H2O_pct` preenchidos;
  - o merge do KIBOX com a tabela final usava apenas `Load_kW + composicao`, o que permitia vazamento entre `subida` e `descida`.
- Correcao aplicada no `nanum_pipeline_28.py`:
  - `_parse_filename_composition()` passou a aceitar `BxxDyy` alem de `DxxByy`;
  - `kibox_aggregate()` passou a agregar arquivos diesel quando `DIES_pct/BIOD_pct` existem;
  - `kibox_mean_row()` passou a carregar `SourceFolder`;
  - o merge KIBOX na tabela final passou a usar:
    - `SourceFolder`
    - `Load_kW`
    - composicao
  - o helper `_normalized_extra_merge_key()` foi adicionado para tratar merge extra numerico ou textual com normalizacao consistente.
- Validacao feita:
  - `Subindo_baseline_2`:
    - `19/19` pontos com `KIBOX_AI05_1` no `lv_kpis_clean.xlsx`;
    - plots `kibox_*` gerados em `out_NANUM\plots\Subindo_baseline_2`.
  - `Descendo_baseline_2`:
    - `0/19` pontos com `KIBOX_AI05_1` enquanto nao houver `_i.csv` correspondente;
    - sem plots `kibox_*` na pasta da descida.
  - o `out_NANUM` foi reprocessado apos a correcao para substituir a execucao anterior com merge incorreto.
- Observacao importante:
  - a pasta `Descendo_baseline_2` ainda contem apenas `.open` de combustao;
  - para ter KIBOX real nessa serie, primeiro e preciso converter esses `.open` para `*_i.csv`.

## Robustez do `standalone_kibox_cycle_viewer_fast.py` na abertura de CSV - 2026-03-11
- Sintomas observados:
  - em alguns PCs com Python `3.12`, a abertura do viewer podia cair no `platform._syscmd_ver()` por problema de code page do Windows;
  - mesmo depois disso, alguns CSVs selecionados manualmente nao abriam porque o loader exigia:
    - separador `tab`;
    - nomes de coluna exatamente `Cycle number`, `Crank angle`, `PCYL_1`, `Q_1`;
    - layout fixo com a segunda linha ignorada.
- Correcao aplicada em `standalone_kibox_cycle_viewer_fast.py`:
  - adicionado fallback seguro para `platform._syscmd_ver()` quando ocorrer `UnicodeDecodeError` no Windows;
  - o loader do CSV passou a:
    - detectar delimitador automaticamente (`tab`, `;`, `,`, `|`);
    - detectar a linha de cabecalho;
    - aceitar variacoes razoaveis de nome nas colunas esperadas, inclusive com unidades no header;
    - converter numericos com virgula ou ponto decimal sem depender de `decimal=","`.
  - quando o arquivo escolhido nao for um CSV KIBOX compativel, o script agora mostra erro legivel com as colunas encontradas, em vez de deixar traceback cru do `pandas`.
- Validacao feita:
  - `--no-show` passou com CSV sintetico em formato:
    - `tab` + linha de unidades;
    - `;` + cabecalho com unidades;
  - CSV propositalmente incompatível retorna mensagem clara de incompatibilidade.

## Feedback de carregamento para CSV KIBOX grande no viewer - 2026-03-11
- Sintoma observado:
  - ao abrir um CSV de traço KIBOX real e grande, por exemplo `C:\Users\SC61730\Downloads\TESTE_50KW_E100-2026-01-17--17-12-46-081.csv`, o terminal mostrava apenas:
    - `[WARN] Input padrao indisponivel: ...`
  - depois disso parecia que nada acontecia, mas o script ainda estava carregando o dataset em memoria.
- Diagnostico:
  - o arquivo citado tem cerca de `315 MB`;
  - o viewer precisa consolidar cerca de `4.320.000` linhas para montar os mapas ciclo a ciclo;
  - em cache quente, o preparo ficou na faixa de `38 s` e a execucao completa em subprocesso ficou na faixa de `50 s`;
  - em leitura fria o tempo percebido pode ser maior.
- Correcao aplicada em `standalone_kibox_cycle_viewer_fast.py`:
  - adicionado `fast path` para o formato KIBOX padrao, lendo apenas:
    - `Cycle number`
    - `Crank angle`
    - `PCYL_1`
    - `Q_1`
  - a deteccao de layout passou a ler apenas uma amostra inicial do arquivo, em vez de carregar o CSV inteiro em bytes antes do parse;
  - o `main()` agora:
    - imprime `[INFO] Loading viewer dataset: ...`;
    - imprime `[INFO] Viewer dataset ready in X.Ys: ...`;
    - abre um `QProgressDialog` modal de `Loading KIBOX CSV...` enquanto o parse pesado roda.
- Otimizacao adicional aplicada depois:
  - o `fast path` deixou de ler as 4 colunas como texto com coercao posterior;
  - para o CSV KIBOX padrao ele agora usa `pd.read_csv(... decimal=\",\" usecols=[...])` direto no parse numerico, mantendo o fallback robusto so para formatos fora do padrao.
- Ganho medido:
  - no arquivo `TESTE_50KW_E100-2026-01-17--17-12-46-081.csv`, `load_cycle_dataframe()` caiu de cerca de `92 s` para cerca de `6,3 s`;
  - o `--no-show` completo caiu para cerca de `8,8 s` no mesmo arquivo.
- Observacao operacional:
  - os arquivos `_m.csv` (MoTeC) e `_i.csv` (KIBOX resumido por ciclo) continuam incompatíveis com esse viewer;
  - para o viewer de `Crank angle`, usar o CSV completo de traço, como os `TESTE_...csv` sem sufixo `_m` ou `_i`.

## Layout 2x2 no `standalone_kibox_cycle_viewer_fast.py` com diagrama P-V - 2026-03-11
- Pedido aplicado no viewer principal:
  - `PCYL_1` por crank angle deixou de ocupar a largura total e passou para o quadrante superior esquerdo;
  - no quadrante superior direito entrou um diagrama `P-V` com:
    - `Volume` no eixo X;
    - `P_CYL` no eixo Y;
    - escala logaritmica no eixo de pressao;
  - o quadrante inferior esquerdo ficou com `Q_1` por crank angle;
  - o quadrante inferior direito ficou com `PMAX` por ciclo.
- Implementacao:
  - o loader passou a exigir tambem a coluna `Volume`;
  - foi criada uma serie dedicada `PVSeries` com:
    - curva do ciclo selecionado;
    - media por bloco no mesmo estilo do overlay existente;
    - limites proprios de `Volume` e de pressao positiva para o eixo log.
  - o diagrama `P-V` passou a usar `log10` nos dois eixos:
    - `Volume` no X;
    - `P_CYL` no Y.
- Validacao feita:
  - `--no-show` no arquivo `C:\Users\SC61730\Downloads\raw_kibox\TESTE_50KW_E100-2026-01-17--17-12-46-081.csv` concluiu com `600` ciclos;
  - instanciacao da UI em `offscreen` confirmou os quatro plots:
    - `pcyl_plot`
    - `pv_plot`
    - `q1_plot`
    - `pmax_plot`.

## Correcao do recarregamento do P-V em `log10-log10` - 2026-03-11
- Sintoma observado:
  - depois de ativar `log10` em `Volume` e `P_CYL`, o quadrante `P-V` podia sumir ao abrir/recarregar arquivos.
- Causa:
  - no `pyqtgraph`, quando `setLogMode(x=True, y=True)` esta ativo, o `ViewBox` trabalha com ranges ja transformados em `log10`;
  - o viewer ainda estava aplicando `setXRange` e `setYRange` com limites lineares ao configurar e ao recarregar dataset.
- Correcao aplicada:
  - criado helper para converter os limites positivos do `P-V` para faixa `log10` antes de chamar `setXRange`/`setYRange`;
  - o ajuste foi aplicado tanto na configuracao inicial quanto no `_apply_dataset()` do recarregamento.
- Validacao feita:
  - teste de recarregar datasets diferentes dentro da mesma instancia confirmou que o range do `P-V` continua cobrindo os `dataBounds` da curva apos `_apply_dataset()`.

## Layout horizontal na aba `Compare` do viewer - 2026-03-11
- Pedido aplicado:
  - os plots comparativos de `PCYL_1` e `Q_1` deixaram de ficar empilhados;
  - agora eles aparecem lado a lado na aba `Compare`.
- Implementacao:
  - `compare_pcyl_plot` ficou em `row=0, col=0`;
  - `compare_q1_plot` passou para `row=0, col=1`.
- Validacao feita:
  - instanciacao em `offscreen` confirmou posicoes distintas na horizontal e curvas carregadas nos dois plots.

## Vazao volumetrica e custo horario no `nanum_pipeline_28.py` - 2026-03-11
- Pedido aplicado no pipeline 28:
  - adicionar vazao convertida de `kg/h` para `L/h` usando densidade por combustivel;
  - adicionar custo em `R$/h` em funcao da potencia total para todos os combustiveis;
  - garantir que essas contas existam primeiro no `lv_kpis_clean.xlsx`, e so depois entrem nos plots.
- Implementacao em `nanum_pipeline_28.py`:
  - criado mapeamento explicito dos blends:
    - `D85B15`
    - `E94H6`
    - `E75H25`
    - `E65H35`
  - `build_final_table()` passou a receber `defaults_cfg` e agora grava no output:
    - `Fuel_Label`
    - `Fuel_Density_kg_m3`
    - `Fuel_Cost_R_L`
    - `Consumo_L_h`
    - `uA_Consumo_L_h`
    - `uB_Consumo_L_h`
    - `uc_Consumo_L_h`
    - `U_Consumo_L_h`
    - `Custo_R_h`
    - `uA_Custo_R_h`
    - `uB_Custo_R_h`
    - `uc_Custo_R_h`
    - `U_Custo_R_h`
  - a conversao usada foi:
    - `Consumo_L_h = Consumo_kg_h * 1000 / densidade_kg_m3`
    - `Custo_R_h = Consumo_L_h * custo_R_L`
  - a propagacao de incerteza foi mantida coerente com o fluxo atual:
    - `L/h` herda a incerteza de `kg/h` escalada por `1000 / densidade`;
    - `R$/h` herda a incerteza de `L/h` escalada por `custo_R_L`;
    - densidade e custo entram como inputs fixos do `Defaults`, sem termo extra de incerteza neste patch.
- Ajuste nos plots:
  - `_fuel_plot_groups()` passou a aceitar tolerancia na comparacao de `H2O_pct` e a rotular os grupos pelo blend identificado;
  - isso permite usar `0,6,25,35` no `Plots` e obter legenda com:
    - `D85B15`
    - `E94H6`
    - `E75H25`
    - `E65H35`
- Atualizacao em `config/config_incertezas_rev3.xlsx`:
  - adicionadas no `Defaults` as entradas:
    - `FUEL_DENSITY_KG_M3_D85B15`
    - `FUEL_DENSITY_KG_M3_E94H6`
    - `FUEL_DENSITY_KG_M3_E75H25`
    - `FUEL_DENSITY_KG_M3_E65H35`
    - `FUEL_COST_R_L_D85B15`
    - `FUEL_COST_R_L_E94H6`
    - `FUEL_COST_R_L_E75H25`
    - `FUEL_COST_R_L_E65H35`
  - adicionadas no `Plots` as linhas:
    - `consumo_l_h_vs_power_all.png`
    - `custo_r_h_vs_power_all.png`
  - ambos usam `Load_kW` no eixo X com a mesma escala dos demais plots all-fuels:
    - `0 .. 55`
    - passo `5`
  - os novos plots usam `filter_h2o_list = 0,6,25,35` para incluir diesel e os tres blends hidratados.
- Observacao operacional:
  - os novos campos do `Defaults` foram criados em branco para preenchimento manual;
  - enquanto densidade e custo nao forem preenchidos, `Consumo_L_h` e `Custo_R_h` permanecem vazios e os plots novos nao terao dados.
- Validacao feita:
  - `python -m py_compile nanum_pipeline_28.py` passou;
  - teste rapido com dataframe sintetico confirmou:
    - identificacao correta de `D85B15`, `E94H6`, `E75H25`, `E65H35`;
    - lookup correto de densidade/custo via `defaults_cfg`;
    - rotulagem correta dos grupos para os plots.

## 2026-03-12 - Pipeline 28: GUI de filtro, economia vs diesel e cenarios de maquinas

- `nanum_pipeline_28.py`:
  - filtro interativo de pontos para plots finalizado com `PySide6`, colunas por combustivel e linhas por carga;
  - abertura do filtro antecipada com base no metadata dos arquivos para reduzir a espera percebida;
  - otimizacoes no diagnostico de qualidade:
    - caminho vetorizado para `TIME_DELTA_REFERENCE_s`;
    - formatacao de tempo reduzida para os pontos realmente usados;
    - `TIME_DIAG_PLOT_DPI = 150`;
    - amostragem de scatter limitada por `TIME_DIAG_FILE_SCATTER_MAX_POINTS = 200`;
  - `build_final_table()` passou a gerar baseline diesel por carga e as colunas:
    - `Diesel_Baseline_Custo_R_h`
    - `Economia_vs_Diesel_R_h`
    - `Economia_vs_Diesel_pct`
    - colunas `uA/uB/uc/U` associadas;
  - adicionados cenarios de maquinas baseados no delta de custo `D85B15` vs `E94H6`, incluindo:
    - custo horario diesel vs etanol;
    - consumo volumetrico diesel vs etanol;
    - consumo anual de etanol;
    - custo anual diesel vs etanol;
    - economia anual vs diesel;
  - o pipeline agora corrige automaticamente parametros de maquinas invertidos no `Defaults` quando detectar `horas/ano` absurdamente baixo e `diesel L/h` absurdamente alto.
- Plots:
  - novos plots configurados em `config/config_incertezas_rev3.xlsx`:
    - `economia_pct_vs_diesel_power_all.png`
    - `economia_r_h_vs_diesel_power_all.png`
  - suite de plots de cenarios de maquinas adicionada ao fluxo final;
  - eixo X desses cenarios alinhado com `Potencia UPD medida (kW, bin 0.1)`;
  - escalas finais ajustadas para:
    - custo horario em `R$/h`;
    - economia horaria em `R$/h`;
    - consumo anual em `x10^3 L/ano`;
    - custo anual em `x10^3 R$/ano`;
  - legenda dos plots de cenario volta ao canto superior esquerdo com folga automatica no eixo Y.
- `config/config_incertezas_rev3.xlsx`:
  - `Defaults` atualizado com parametros das maquinas:
    - `MACHINE_HOURS_PER_YEAR_COLHEITADEIRA = 3150`
    - `MACHINE_DIESEL_L_H_COLHEITADEIRA = 34`
    - `MACHINE_HOURS_PER_YEAR_TRATOR_TRANSBORDO = 1675`
    - `MACHINE_DIESEL_L_H_TRATOR_TRANSBORDO = 12.1`
    - `MACHINE_HOURS_PER_YEAR_CAMINHAO = 4800`
    - `MACHINE_DIESEL_L_H_CAMINHAO = 41`
- Validacoes feitas:
  - `python -m py_compile nanum_pipeline_28.py`;
  - verificacao numerica do `lv_kpis_clean.xlsx` confirmou:
    - `Consumo_L_h = Consumo_kg_h * 1000 / densidade`;
    - `Custo_R_h = Consumo_L_h * custo_R_L`;
    - economias negativas vs diesel para os blends etanolicos;
  - os plots de cenario foram regenerados localmente apos a correcao dos parametros das maquinas.

## 2026-03-13 - Fechamento do dia no `pipeline29`

- Escopo consolidado hoje:
  - congelamento do `pipeline28` e abertura do `pipeline29`;
  - migracao da configuracao principal do Excel para backend textual + GUI;
  - refinamentos extensivos da GUI de configuracao;
  - migracao do legado de `lhv.csv` para `fuel_properties.toml`;
  - persistencia do filtro de pontos para plots;
  - implementacao de `ETA_V_pct`.

- Commits produzidos hoje antes do fechamento final:
  - `37f873a` `Freeze pipeline 28 and branch pipeline 29`
  - `4da935c` `Add text config backend and GUI for pipeline 29`
  - `e7e8dfc` `Improve pipeline 29 config launch flow and editor`
  - `cc1318a` `Normalize text config keys for pipeline 29`
  - `ce1ba28` `Add vertical helpers for pipeline 29 config rows`
  - `d7d37cf` `Refine pipeline 29 config helper defaults`
  - `86e8a2c` `Fix pipeline 29 window state and column sizing`
  - `99963ce` `Improve pipeline 29 plot helper defaults`
  - `5dc66c8` `Refine pipeline 29 plot axis preview`
  - `88f48de` `Support unit-aware plot y steps`
  - `aa9ceb2` `Snap auto y axis to manual step`
  - `843f9ab` `Ignore LabVIEW -1000 pressure sentinels`
  - `3badf33` `Respect manual y axis bounds in plot helper`
  - `1efcbcc` `Add helper row editing and Save & Run to pipeline 29`
  - `e533873` `Migrate fuel LHV config into pipeline 29 text backend`

- Estado final esperado apos este handoff:
  - `pipeline29` usa `config/pipeline29_text/` como fonte principal de configuracao;
  - a GUI cobre:
    - `Defaults`
    - `Data Quality`
    - `Mappings`
    - `Instruments`
    - `Reporting`
    - `Fuel Properties`
    - `Plots`
  - o Excel `config_incertezas_rev3.xlsx` fica como importador legado/bootstrap;
  - `config/lhv.csv` tambem vira legado/fallback, porque a fonte editavel principal passa a ser `config/pipeline29_text/fuel_properties.toml`.

- `Fuel Properties`:
  - arquivo novo versionado:
    - `config/pipeline29_text/fuel_properties.toml`
  - conteudo atual:
    - `D85B15`
    - `E94H6`
    - `E75H25`
    - `E65H35`
  - campos tratados:
    - composicao
    - `LHV_kJ_kg` / PCI
    - densidade
    - custo
    - referencia
    - notas
  - fallback:
    - se `fuel_properties.toml` faltar ou vier vazio, o runtime ainda tenta `config/lhv.csv`.

- Filtro de pontos para plots:
  - o estado do ultimo filtro agora fica salvo localmente em:
    - `LOCALAPPDATA\\nanum_pipeline_29\\plot_point_filter_last.json`
  - comportamento:
    - ao abrir, a ultima selecao compativel e carregada por padrao;
    - pontos novos que nao existiam antes entram marcados;
    - a janela deixa isso visivel e ainda permite revisar antes de rodar;
    - botoes:
      - `Selecionar tudo`
      - `Limpar tudo`
      - `Carregar ultima`
      - `Salvar atual`
  - importante:
    - esse estado local nao e versionado no git.

- Eficiencia volumetrica:
  - colunas novas geradas em runtime:
    - `VOL_EFF_AIR_kg_h_USED`
    - `VOL_EFF_THEORETICAL_AIR_kg_h`
    - `VOL_EFF_RHO_REF_kg_m3`
    - `VOL_EFF_RPM_USED`
    - `VOL_EFF_AIR_SOURCE`
    - `ETA_V`
    - `ETA_V_pct`
  - defaults atuais em `config/pipeline29_text/defaults.toml`:
    - `ENGINE_DISPLACEMENT_L = 3.992`
    - `VOL_EFF_REF_PRESSURE_kPa = 101.3`
    - `VOL_EFF_RPM_COL = Rotação_mean_of_windows`
    - `VOL_EFF_DIESEL_MAF_COL = MAF_mean_of_windows`
    - `VOL_EFF_DIESEL_MAF_MIN_KGH = 0`
    - `VOL_EFF_DIESEL_MAF_MAX_KGH = 300`
  - regra especial do diesel:
    - para `D85B15`, usar `MAF_mean_of_windows`;
    - se `MAF` estiver estatico ou fora de `0..300 kg/h`, cancelar `ETA_V` no diesel;
    - para os demais combustiveis, usar a coluna de ar derivada pelo pipeline.
  - plot novo textual:
    - `eta_v_pct_vs_power_all.png`
  - mapping novo:
    - `ETA_V_pct`

- Salvar/rodar na GUI:
  - `Save` sobrescreve a config ativa;
  - `Save As` troca a pasta de config ativa;
  - `Save & Run` salva e fecha a GUI, retornando para o `pipeline29` continuar o processamento.

- Edicao de linha na GUI:
  - duplo clique em linha preenchida de:
    - `Mappings`
    - `Instruments`
    - `Plots`
    abre o helper vertical de edicao, sem precisar editar celula por celula na grade.

- Observacao sobre os TOMLs atuais:
  - a GUI atual salva muitos campos como string, inclusive numeros;
  - placeholders vazios podem aparecer como `"nan"` em `plots.toml`, `instruments.toml` e `reporting_rounding.toml`;
  - isso esta compatibilizado com o parser atual, mas e um ponto claro para limpeza futura se quiser um texto mais bonito/manual.

- Validacao realizada para este fechamento:
  - `.\.venv\Scripts\python.exe -m py_compile nanum_pipeline_29.py`
  - `.\.venv\Scripts\python.exe -m py_compile pipeline29_config_backend.py`
  - `.\.venv\Scripts\python.exe -m py_compile pipeline29_config_gui.py`
  - smoke tests locais cobrindo:
    - helper de edicao por duplo clique;
    - `Save & Run`;
    - bundle textual com `Fuel Properties`;
    - fallback para config sem `fuel_properties.toml`;
    - persistencia do filtro de pontos;
    - cancelamento de `ETA_V` no diesel com `MAF` estatico ou fora da faixa.

- Ponto que nao foi revalidado end-to-end nesta ultima rodada:
  - nao houve rerun completo do processamento real para regenerar `lv_kpis_clean.xlsx` e todos os plots apos o patch final de `ETA_V_pct` e do filtro persistente.

## Publicacao do pipeline29 e split do conversor Kibox - 2026-03-16
- Objetivo:
  - fechar a defasagem entre o estado local do `pipeline29` e o Git remoto;
  - consolidar a estrategia de repositorios para o conversor `kibox_open_to_csv`.
- Repositorios envolvidos:
  - `Processamentos`
  - `Knock_Distribution`
  - `kibox_open_to_csv`
- Decisoes tomadas:
  - `Processamentos` continua sendo a casa do `pipeline28`/`pipeline29` e da copia operacional local do wrapper Kibox.
  - O repositorio canonico do conversor passa a ser `https://github.com/Motterdude/kibox_open_to_csv`.
  - `Knock_Distribution` continua podendo manter uma copia espelhada do wrapper em `tools/`, mas a referencia principal de manutencao do conversor deixa de ser o repo de Knock.
- Impacto operacional:
  - quem quiser evoluir apenas o fluxo `.open -> .csv` deve partir do repo dedicado;
  - o `Processamentos` segue pronto para rodar sem depender de clone adicional do conversor;
  - documentacao e handoffs passam a apontar explicitamente para a separacao entre pipeline, Knock e conversor Kibox.
- Pendencia fechada neste handoff:
  - o `pipeline29`, que estava apenas commitado localmente em `Processamentos`, entra na rodada de sincronizacao para o Git remoto junto dos demais artefatos atuais.
