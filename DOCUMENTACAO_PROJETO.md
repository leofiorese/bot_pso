# Documentação do projeto PSOffice Bot

## Visão geral

O projeto automatiza a coleta de relatórios personalizados do PSOffice, salva os arquivos baixados, processa os dados e realiza operações de upsert em um banco MySQL. Ele oferece duas formas de uso: uma interface gráfica em Tkinter para interação manual ou automatizada, e um fluxo opcional de geração de insights com IA a partir de consultas SQL sobre os dados carregados.

## Componentes principais

### Automação de relatórios (app/main.py)
- Carrega variáveis de ambiente (.env) com URLs de login e relatório do PSOffice, credenciais e modo headless do navegador Playwright. Define diretórios de download e configura o logger (`pso_bot.log`).
- Implementa `do_login`, que navega até a tela de login, aceita o aviso de cookies quando presente e envia as credenciais antes de aguardar a estabilização da sessão.
- A função `goto_report` seleciona e insere o script SQL apropriado em um campo de texto, aciona o botão **Testar (EXCEL)**, captura o download sugerido e salva o arquivo em `app/downloads` com timestamp.
- O dicionário `SCRIPT_GENERATORS` despacha a construção das consultas SQL para cada tipo de relatório (Orçado, Planejado, Realizado e diversas tabelas auxiliares), enquanto `UPSERT_HANDLERS` indica como cada DataFrame deve ser gravado no banco.
- `run_once` executa o fluxo completo: login, navegação, download, leitura do CSV, validação do número de colunas, upsert e limpeza do arquivo. Ele pode rodar em **modo automático** (percorre todos os tipos de relatório) ou **manual** (um tipo escolhido via GUI), com três tentativas e intervalos entre execuções. Também suporta um deslocamento de datas configurável via `DATEADD` na query.

### Interface gráfica (app/gui.py)
- Janela fullscreen em Tkinter com três ações principais: buscar um único relatório personalizado, executar o ciclo completo de todos os relatórios ou gerar insights com IA.
- Fluxo para relatórios: solicita o tipo de script (timeout de 10s com fallback para "Orçado"), pergunta se o usuário quer alterar o recorte de datas e dispara `run_once` em uma thread para não travar a interface. Há detecção de inatividade que dispara automaticamente a coleta automática após 10s sem interação.
- Exibe e atualiza periodicamente o arquivo de log dentro da interface, além de botões para limpar log e fechar o app. Salva as últimas entradas de SQL e prompt em `last_inputs.json` para reutilização.

### Processamento e validação do CSV (app/actions/process_csv/process_csv.py)
- Lê arquivos CSV em `latin1` usando delimitador `;`.
- Confere se o tipo de relatório recebido existe no mapa `TABLE_MAP` e se o número de colunas do arquivo coincide com o esperado para aquela tabela (definido nos módulos de upsert). Em caso de divergência gera erro explícito.
- Retorna um DataFrame pandas pronto para o upsert.

### Persistência dos dados (app/actions/upsert_data)
- Cada módulo `upsert_*` define as colunas esperadas, SQL de criação da tabela e comando de upsert específico. O fluxo padrão: abrir conexão MySQL (garantindo a existência do banco), criar/verificar tabela, normalizar tipos (datas, booleanos, nulos), inserir/atualizar linhas e remover o CSV após sucesso.
- A conexão é obtida via `db/db.py`, que carrega configurações do .env e cria o banco se necessário antes de retornar uma conexão MySQL.
- O módulo `upsert_insights_llm.py` salva as análises produzidas pela IA em `RELATORIO_PSO_INSIGHTS_LLM`, extraindo o JSON de um bloco ```json``` da resposta, validando o `PROJ_ID` obrigatório e convertendo listas em texto Markdown para campos descritivos.

### Geração de SQL
- Os scripts em `app/sql_scripts` produzem consultas parametrizadas com `DATEADD`, permitindo alterar o recorte temporal em dias. O `SCRIPT_GENERATORS` do `main.py` seleciona o gerador adequado com base no relatório solicitado.

### Insights com IA (app/ia.py)
- Executa uma consulta SQL fornecida pela GUI (`query_to_dataframe`), divide DataFrames grandes em blocos de até 1000 linhas e cria um prompt estruturado com regras de negócio, métricas e formato de resposta JSON.
- Envia cada bloco para o modelo `gpt-oss:20b` via Ollama, registra logs detalhados e salva cada resposta processada no banco usando `upsert_insights_llm`.

### Configurações e diretórios
- Arquivo `.env` deve definir `PSO_LOGIN_URL`, `PSO_REPORT_URL`, `PSO_USERNAME`, `PSO_PASSWORD`, `HEADLESS` e parâmetros do MySQL (`MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DB`).
- `app/downloads` é criado automaticamente para armazenar os relatórios baixados; `pso_bot.log` guarda os registros do fluxo.
- `config_default_script.py` mantém o tipo de relatório padrão selecionado pela GUI.

## Como executar
1. Configure as variáveis de ambiente no `.env` com acesso ao PSOffice e ao banco MySQL.
2. Instale as dependências (Python, Playwright com Firefox, drivers MySQL, Ollama se for usar IA).
3. Execute `python -m app.gui` (ou `python app/gui.py`) para abrir a interface. Escolha entre modo automático, relatório único ou geração de insights.
4. Acompanhe o progresso no painel de logs da interface ou no arquivo `pso_bot.log`.

## Fluxo resumido
1. Usuário inicia a coleta pela GUI → define tipo de relatório e recorte de datas.
2. Playwright faz login no PSOffice, executa a query SQL correspondente e baixa o Excel/CSV.
3. O CSV é lido pelo pandas, validado e transformado; o módulo de upsert cria/atualiza tabelas MySQL e remove o arquivo.
4. Opcionalmente, o usuário executa uma query customizada, envia o DataFrame para a IA e grava os insights estruturados no banco.
