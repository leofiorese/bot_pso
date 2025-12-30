# Documentação Técnica - PSOffice Bot

> **Bot de automação para extração, processamento e análise de relatórios do sistema PSO (PSOffice)**

---

## 1. Visão Geral da Arquitetura (High-Level Design)

### Padrão Arquitetural

O projeto segue uma arquitetura **Modular Monolítica** com separação clara de responsabilidades:

| Camada | Descrição |
|--------|-----------|
| **Presentation** | GUI desktop via Tkinter |
| **Orchestration** | `main.py` coordena fluxos e despacho de scripts |
| **Data Access** | Módulos `upsert_data` e `db.py` para persistência MySQL |
| **Integration** | Playwright para web scraping, Ollama para IA |
| **Utilities** | `utils.py` para funções auxiliares (arquivamento de CSVs) |

### Stack Tecnológico

| Componente | Tecnologia |
|------------|------------|
| **Linguagem** | Python 3.x |
| **GUI Desktop** | Tkinter |
| **Web Automation** | Playwright (Firefox) |
| **Banco de Dados** | MySQL 8.x (via `mysql-connector-python`, SQLAlchemy) |
| **IA/LLM** | Ollama (modelo `gpt-oss:20b` com `think=medium`) |
| **Data Processing** | Pandas |
| **Configuração** | python-dotenv (`.env`) |
| **Logging** | logging (arquivo `pso_bot.log`) |

### Diagrama de Arquitetura

```mermaid
graph TD
    subgraph "Presentation Layer"
        GUI[GUI Tkinter<br/>gui.py]
    end

    subgraph "Orchestration Layer"
        MAIN[Orchestrator<br/>main.py]
        CONFIG[Config<br/>config_default_script.py]
    end

    subgraph "Integration Layer"
        PLAYWRIGHT[Playwright<br/>Firefox Browser]
        OLLAMA[Ollama LLM<br/>ia.py]
    end

    subgraph "Data Layer"
        PROCESS[process_csv.py]
        UPSERT[Upsert Handlers<br/>26 módulos]
        QUERY[query_to_dataframe.py]
        DB[db.py<br/>MySQL Connector]
    end

    subgraph "Utilities Layer"
        UTILS[utils.py<br/>arquivar_csv]
    end

    subgraph "External Systems"
        PSO[PSOffice Web<br/>Relatórios SQL]
        MYSQL[(MySQL<br/>Database)]
        REDE[Diretório de Rede<br/>Z:\...\BASE]
    end

    GUI --> MAIN
    GUI --> OLLAMA
    MAIN --> CONFIG
    MAIN --> PLAYWRIGHT
    PLAYWRIGHT --> PSO
    PSO -->|CSV/XLSX| PROCESS
    PROCESS --> UPSERT
    UPSERT --> DB
    UPSERT --> UTILS
    UTILS --> REDE
    DB --> MYSQL
    OLLAMA --> QUERY
    QUERY --> DB
    OLLAMA -->|Insights JSON| UPSERT
```

---

## 2. Mapeamento de Entidades e Banco de Dados

### Entidades de Domínio

O sistema trabalha com **25 entidades** extraídas do PSO, divididas em:

| Categoria | Entidades |
|-----------|-----------|
| **Relatórios Principais** | `RELATORIO_PSO_REALIZADO`, `RELATORIO_PSO_ORCADO`, `RELATORIO_PSO_PLANEJADO` |
| **Estrutura Organizacional** | `PROJETOS`, `ATIVIDADES`, `RECURSOS`, `EMPRESAS`, `CENTROS_DE_RESULTADO` |
| **Alocação de Pessoas** | `APONTAMENTOS`, `ATRIBUICOES`, `INFO_COLABS`, `PSO_USU_FUNCOES`, `RELATORIO_DE_COLABORADORES` |
| **Financeiro** | `FATURAMENTO`, `DESPESAS`, `DESPESA_ORCADA`, `DESPESA_TIPO`, `PSO_TAXA`, `TAXA_HISTORICO` |
| **Tempo/Calendário** | `CALENDARIOS`, `D_CALEND_PROJ`, `RESUMO_DE_HORAS`, `RESUMO_DE_HORAS_ATIV` |
| **Agrupamento** | `AGRUPAMENTO`, `GRREF` |
| **IA/Insights** | `RELATORIO_PSO_INSIGHTS_LLM` |

### Diagrama ER Simplificado

```mermaid
erDiagram
    PROJETOS {
        int PROJ_ID PK
        int DEPT_ID
        int EMP_ID FK
        int CLI_ID
        varchar CODIGO
        varchar NOME
        decimal VALOR
        date DT_INICIO
        date DT_FIM
    }
    
    ATIVIDADES {
        int ATIV_ID PK
        int PROJ_ID FK
        varchar NOME
        decimal TRABALHO_PREVISTO
        decimal TRABALHO_APONTADO
        date DT_INICIO
        date DT_FIM
    }
    
    APONTAMENTOS {
        int APON_ID PK
        int USU_ID FK
        int ATIV_ID FK
        int PROJ_ID FK
        datetime DT_INICIO
        int MINUTOS
        int STATUS
    }
    
    RECURSOS {
        int USU_ID PK
        varchar NOME
        varchar EMAIL
        int TAXA_ID FK
        boolean ATIVO
    }

    RELATORIO_DE_COLABORADORES {
        varchar Login PK
        varchar Nome
        varchar Sigla
        varchar Email
        varchar CPF
        varchar RG
        date DataAdmissao
        date DataNascimento
        varchar NomeCentroResultado
        varchar NomePessoaJuridica
        decimal ValorTaxaHistorico
    }
    
    ATRIBUICOES {
        int ATRIB_ID PK
        int ATIV_ID FK
        int USU_ID FK
        decimal TRABALHO
    }
    
    FATURAMENTO {
        int FAT_ID PK
        int PROJ_ID FK
        decimal VALOR
        date DT_FATURAMENTO
    }
    
    DESPESAS {
        int DESP_ID PK
        int PROJ_ID FK
        decimal VALOR
        varchar TIPO
    }
    
    INSIGHTS_LLM {
        int id_insight PK
        int PROJ_ID FK
        json analise_resumida_json
        text insights_acionaveis_md
        text recomendacoes_md
    }

    PROJETOS ||--o{ ATIVIDADES : "contém"
    PROJETOS ||--o{ APONTAMENTOS : "registra"
    PROJETOS ||--o{ FATURAMENTO : "gera"
    PROJETOS ||--o{ DESPESAS : "incorre"
    PROJETOS ||--o{ INSIGHTS_LLM : "produz"
    ATIVIDADES ||--o{ APONTAMENTOS : "recebe"
    ATIVIDADES ||--o{ ATRIBUICOES : "aloca"
    RECURSOS ||--o{ APONTAMENTOS : "realiza"
    RECURSOS ||--o{ ATRIBUICOES : "é alocado"
```

---

## 3. Documentação Detalhada por Módulo

### 3.1 Camada de Orquestração

#### [main.py](file:///c:/Users/leonardo.fiorese/Documents/bot_pso/app/main.py)

| Aspecto | Descrição |
|---------|-----------|
| **Responsabilidade** | Orquestrador principal que coordena login, execução de queries SQL, download de relatórios e upsert no banco |
| **Principais Funções** | `run_once()` - executa fluxo completo; `do_login()` - autenticação PSO; `goto_report()` - navega e baixa relatório |
| **Dependências** | Playwright, todos os upsert handlers, todos os sql_scripts |
| **Padrões** | **Strategy Pattern** (dicionários `SCRIPT_GENERATORS` e `UPSERT_HANDLERS` para despacho dinâmico) |

```python
# Exemplo de Strategy Pattern (25 estratégias)
SCRIPT_GENERATORS = {
    "Orçado": gerar_script_final_orcado,
    "Planejado": gerar_script_final_planejado,
    "Realizado": gerar_script_final_realizado,
    "RELATORIO_DE_COLABORADORES": gerar_script_final_relatorio_de_colaboradores,
    # ... 21 outras estratégias
}
```

**Modos de Execução:**
- **Automático** (`user_choice=0`): Executa 22 relatórios em sequência (excluindo Orçado, Planejado, Realizado)
- **Manual** (`user_choice=1`): Executa apenas o relatório selecionado pelo usuário

---

#### [gui.py](file:///c:/Users/leonardo.fiorese/Documents/bot_pso/app/gui.py)

| Aspecto | Descrição |
|---------|-----------|
| **Responsabilidade** | Interface gráfica desktop que permite usuário escolher relatórios, configurar datas e iniciar processos |
| **Principais Funções** | `create_main_window()` - janela principal; `ask_for_script_choice()` - seleção de relatório; `ask_for_sql_query()` - input para IA |
| **Dependências** | Tkinter, main.py, ia.py |
| **Padrões** | **Observer Pattern** (detecção de inatividade com `check_inactivity()`) |

**Features:**
- Detecção automática de inatividade (10s) → executa modo automático
- Persistência de últimos inputs em `last_inputs.json`
- Log viewer em tempo real (atualização a cada 3s)
- 25 opções de relatórios via RadioButton
- Três botões principais: Pesquisa Personalizada, Pesquisa Automática, Gerar Insights com IA

---

### 3.2 Camada de Integração

#### [ia.py](file:///c:/Users/leonardo.fiorese/Documents/bot_pso/app/ia.py)

| Aspecto | Descrição |
|---------|-----------|
| **Responsabilidade** | Integração com LLM (Ollama) para geração de insights analíticos a partir de DataFrames |
| **Principais Funções** | `generate_insights()` - envia dados para LLM; `dataframe_to_text()` - monta prompt estruturado; `split_dataframe()` - divide DF em chunks |
| **Dependências** | Ollama, Pandas, upsert_insights_llm |
| **Padrões** | **Template Method** (prompt estruturado com seções fixas) |

**Modelos Disponíveis:**
- `gpt-oss:20b` + think = medium (atual)
- `llama3.1:8b`
- `gemma3:12b`
- `magistral:24b`

**Prompt Engineering:**
- Define papel: "Analista de Dados e Projetos Sênior"
- Premissas de negócio (8h/dia, 40h/semana)
- Output esperado: JSON estruturado com 5 seções obrigatórias:
  - `chaves_identificadoras`
  - `analise_resumida`
  - `insights_acionaveis`
  - `pontos_de_atencao`
  - `recomendacoes`

---

### 3.3 Camada de Utilitários

#### [utils.py](file:///c:/Users/leonardo.fiorese/Documents/bot_pso/app/utils.py)

| Aspecto | Descrição |
|---------|-----------|
| **Responsabilidade** | Funções auxiliares para operações comuns do sistema |
| **Principais Funções** | `arquivar_csv()` - move CSV processado para diretório de rede |
| **Dependências** | os, shutil, logging |

**Função `arquivar_csv()`:**
- Move arquivo CSV para `Z:\3-Corporativo\PMO\...\BASE`
- Renomeia arquivo para o nome da tabela (ex: `RELATORIO_DE_COLABORADORES.csv`)
- Sobrescreve arquivo anterior automaticamente
- Cria diretório destino se não existir

---

### 3.4 Camada de Dados

#### [db.py](file:///c:/Users/leonardo.fiorese/Documents/bot_pso/app/db/db.py)

| Aspecto | Descrição |
|---------|-----------|
| **Responsabilidade** | Gerenciamento de conexão com MySQL, criação automática de database |
| **Principais Funções** | `get_conn()` - retorna conexão; `_ensure_database_exists()` - cria DB se não existir |
| **Dependências** | mysql-connector-python, python-dotenv |
| **Padrões** | **Factory Pattern** (factory de conexões) |

---

#### [process_csv.py](file:///c:/Users/leonardo.fiorese/Documents/bot_pso/app/actions/process_csv/process_csv.py)

| Aspecto | Descrição |
|---------|-----------|
| **Responsabilidade** | Leitura e validação de arquivos CSV baixados do PSO |
| **Principais Funções** | `process_csv()` - lê CSV e valida número de colunas contra schema esperado |
| **Dependências** | Pandas, todos os TABLE_COLUMNS dos upsert handlers |
| **Padrões** | **Registry Pattern** (dicionário `TABLE_MAP` mapeia tipo → schema) |

---

#### Upsert Handlers (26 módulos)

Todos seguem a mesma estrutura:

```python
# Estrutura padrão de cada upsert handler
TABLE_COLUMNS = [...]       # Schema esperado
CREATE_TABLE_SQL = """...""" # DDL de criação
UPSERT_SQL = """..."""       # INSERT ON DUPLICATE KEY UPDATE

def upsert_data(df, table_name, csv_path):
    # 1. Cria tabela se não existir
    # 2. Limpa e converte dados (datas, booleans)
    # 3. Executa upsert row-by-row
    # 4. Arquiva CSV na rede (via arquivar_csv)
```

| Padrão | Descrição |
|--------|-----------|
| **Repository Pattern** | Cada handler encapsula acesso a uma entidade específica |
| **UPSERT Idempotente** | `INSERT ... ON DUPLICATE KEY UPDATE` garante idempotência |

**Lista Completa de Handlers (26):**

| Handler | Tabela | Chave Primária |
|---------|--------|----------------|
| `upsert_realizado_data.py` | RELATORIO_PSO_REALIZADO | Composta |
| `upsert_orcado_data.py` | RELATORIO_PSO_ORCADO | Composta |
| `upsert_planejado_data.py` | RELATORIO_PSO_PLANEJADO | Composta |
| `upsert_agrupamento.py` | AGRUPAMENTO | - |
| `upsert_apontamentos.py` | APONTAMENTOS | APON_ID |
| `upsert_atividades.py` | ATIVIDADES | ATIV_ID |
| `upsert_atribuicoes.py` | ATRIBUICOES | ATRIB_ID |
| `upsert_calendarios.py` | CALENDARIOS | - |
| `upsert_centros_de_resultado.py` | CENTROS_DE_RESULTADO | CR_ID |
| `upsert_d_calend_proj.py` | D_CALEND_PROJ | - |
| `upsert_despesa_orcada.py` | DESPESA_ORCADA | - |
| `upsert_despesa_tipo.py` | DESPESA_TIPO | - |
| `upsert_despesas.py` | DESPESAS | DESP_ID |
| `upsert_empresas.py` | EMPRESAS | EMP_ID |
| `upsert_faturamento.py` | FATURAMENTO | FAT_ID |
| `upsert_grref.py` | GRREF | - |
| `upsert_info_colabs.py` | INFO_COLABS | USU_ID |
| `upsert_insights_llm.py` | RELATORIO_PSO_INSIGHTS_LLM | id_insight |
| `upsert_projetos.py` | PROJETOS | PROJ_ID |
| `upsert_pso_taxa.py` | PSO_TAXA | TAXA_ID |
| `upsert_pso_usu_funcoes.py` | PSO_USU_FUNCOES | - |
| `upsert_recursos.py` | RECURSOS | USU_ID |
| `upsert_relatorio_de_colaboradores.py` | RELATORIO_DE_COLABORADORES | Login |
| `upsert_resumo_de_horas.py` | RESUMO_DE_HORAS | - |
| `upsert_resumo_de_horas_ativ.py` | RESUMO_DE_HORAS_ATIV | - |
| `upsert_taxa_historico.py` | TAXA_HISTORICO | - |

---

### 3.5 SQL Scripts (25 módulos)

Cada script em `sql_scripts/` gera queries parametrizadas para o PSO:

```python
# Exemplo: relatorio_de_colaboradores_script.py
def gerar_script_final(dateadd_string):
    return f"""
    SELECT usuarios.LOGIN AS Login, usuarios.NOME AS Nome, ...
    FROM PSO_USUARIOS usuarios
    LEFT JOIN PSO_CENTROS_RESULTADO cr ON usuarios.CR_ID = cr.CR_ID
    LEFT JOIN PSO_PESSOAS_JURIDICAS pj ON usuarios.EMP_ID = pj.PJ_ID
    LEFT JOIN PSO_TAXA taxa ON usuarios.TAXA_ID_CUS = taxa.TAXA_ID
    LEFT JOIN PSO_TAXA_HISTORICO taxa_historico ON taxa.TAXA_ID = taxa_historico.TAXA_ID;
    """
```

---

## 4. Fluxos Críticos de Negócio

### 4.1 Fluxo de Extração Automática de Relatórios

```mermaid
sequenceDiagram
    participant U as Usuário
    participant G as GUI (Tkinter)
    participant M as main.py
    participant P as Playwright
    participant PSO as PSOffice Web
    participant CSV as process_csv
    participant DB as MySQL
    participant REDE as Diretório de Rede

    U->>G: Clica "Iniciar Pesquisa Automática"
    G->>M: run_once(user_choice=0)
    
    loop Para cada tipo de relatório (22x)
        M->>P: launch(firefox, headless)
        P->>PSO: goto(LOGIN_URL)
        P->>PSO: fill(username, password)
        P->>PSO: click(submit)
        P->>PSO: goto(REPORT_URL)
        M->>M: gerar_script_final(dateadd)
        P->>PSO: fill(textarea, SQL)
        P->>PSO: click("Testar EXCEL")
        PSO-->>P: download(arquivo.xlsx)
        P-->>M: csv_file_path
        M->>CSV: process_csv(path, tipo)
        CSV-->>M: DataFrame validado
        M->>DB: upsert_data(df, tabela)
        M->>REDE: arquivar_csv(path, tabela)
        M->>M: sleep(5s)
    end
    
    M-->>G: Processo concluído (log)
```

---

### 4.2 Fluxo de Geração de Insights com IA

```mermaid
sequenceDiagram
    participant U as Usuário
    participant G as GUI
    participant IA as ia.py
    participant Q as query_to_dataframe
    participant DB as MySQL
    participant LLM as Ollama (gpt-oss:20b)
    participant UPS as upsert_insights_llm

    U->>G: Clica "Gerar Insights com IA"
    G->>G: ask_for_sql_query()
    U->>G: Digita SQL customizado
    G->>G: ask_for_user_prompt()
    U->>G: Define escopo de análise
    G->>G: ask_for_acknowledgment()
    U->>G: Confirma "Estou Ciente"
    G->>IA: generate_insights(df, prompt)
    IA->>Q: query_to_dataframe(sql)
    Q->>DB: SELECT ...
    DB-->>Q: ResultSet
    Q-->>IA: DataFrame
    
    loop Para cada chunk (max 1000 rows)
        IA->>IA: dataframe_to_text(chunk)
        IA->>LLM: chat(prompt, think="medium")
        LLM-->>IA: JSON (análise, insights, recomendações)
        IA->>UPS: upsert_data(json_response)
        UPS->>DB: INSERT INTO INSIGHTS_LLM
    end
    
    IA-->>G: Insights gerados (log)
```

---

### 4.3 Fluxo de Persistência (Upsert Pattern)

```mermaid
flowchart LR
    A[DataFrame Pandas] --> B{Validar Schema}
    B -->|OK| C[Limpar Dados]
    B -->|Erro| X[Raise ValueError]
    C --> D[Converter Datas]
    D --> E[Converter Booleans Y/N]
    E --> F[Substituir NaN por NULL]
    F --> G[Loop por Row]
    G --> H[INSERT ON DUPLICATE KEY UPDATE]
    H --> I{Commit}
    I -->|Sucesso| J[Arquivar CSV na Rede]
    J --> K[Deletar CSV Local]
    I -->|Erro| L[Rollback]
```

---

## 5. Avaliação de Engenharia (Code Review)

### ✅ Pontos Fortes

| Aspecto | Descrição |
|---------|-----------|
| **Modularidade** | Separação clara: cada tabela tem seu próprio handler de upsert |
| **Idempotência** | Uso consistente de `INSERT ... ON DUPLICATE KEY UPDATE` |
| **Logging Robusto** | Todas as operações são logadas em `pso_bot.log` |
| **Resiliência** | Retry com backoff exponencial (`MAX_RETRIES = 3`, `sleep(4 * i)`) |
| **Backup Automático** | CSVs são arquivados na rede após processamento |
| **Configuração Externa** | `.env` para credenciais e URLs (não hardcoded) |
| **Auto-criação de DB** | `_ensure_database_exists()` cria banco se necessário |
| **Persistência de Inputs** | Últimos inputs SQL/prompt salvos em `last_inputs.json` |

---

### ⚠️ Pontos de Atenção

| Categoria | Issue | Recomendação |
|-----------|-------|--------------|
| **Performance** | Upsert row-by-row via loop Python | Usar `executemany()` ou bulk insert |
| **SQL Injection** | Scripts SQL usam f-strings com `dateadd_string` | Parametrizar queries |
| **Duplicação de Código** | 26 upsert handlers com estrutura idêntica | Criar classe base `BaseUpsertHandler` |
| **Error Handling** | `except Exception as e` genérico | Capturar exceções específicas |
| **Memory** | DataFrame inteiro em memória | Processar em chunks para arquivos grandes |
| **Hardcoded** | `timeout=60_000` e seletores CSS em `main.py` | Externalizar para config |
| **Hardcoded** | Caminho de rede em `utils.py` | Externalizar para `.env` |
| **Testing** | Sem testes unitários visíveis | Adicionar pytest com mocks |
| **Type Hints** | Ausentes em boa parte do código | Adicionar typing para manutenibilidade |

---

### 🔒 Considerações de Segurança

| Risco | Mitigação Atual | Recomendação |
|-------|-----------------|--------------|
| Credenciais em `.env` | `.gitignore` impede commit | OK, adicionar vault para produção |
| SQL dinâmico | f-strings | Parametrizar via prepared statements |
| Logs com dados sensíveis | Prompt completo logado | Sanitizar logs sensíveis |

---

## Anexo: Estrutura de Diretórios

```
bot_pso/
├── .env                          # Credenciais (ignorado no git)
├── .gitignore
├── README.md
├── DOCUMENTACAO_PROJETO.md       # Este arquivo
├── pso_bot.log                   # Arquivo de log
├── last_inputs.json              # Últimos inputs do usuário (IA)
├── data_csv/                     # CSVs temporários
└── app/
    ├── main.py                   # Orquestrador principal (317 linhas)
    ├── gui.py                    # Interface Tkinter (494 linhas)
    ├── ia.py                     # Integração Ollama LLM (202 linhas)
    ├── utils.py                  # Funções utilitárias (43 linhas)
    ├── config_default_script.py  # Configuração de script padrão
    ├── db/
    │   └── db.py                 # Conexão MySQL
    ├── downloads/                # Arquivos baixados do PSO
    ├── sql_scripts/              # 25 geradores de SQL
    │   ├── realizado_script.py
    │   ├── orcado_script.py
    │   ├── planejado_script.py
    │   ├── relatorio_de_colaboradores_script.py
    │   └── ... (21 outros scripts)
    └── actions/
        ├── process_csv/
        │   └── process_csv.py    # Leitor/validador CSV
        ├── query_to_dataframe/
        │   └── query_to_dataframe.py
        └── upsert_data/          # 26 handlers de persistência
            ├── upsert_realizado_data.py
            ├── upsert_projetos.py
            ├── upsert_insights_llm.py
            ├── upsert_relatorio_de_colaboradores.py
            └── ... (22 outros handlers)
```

---

*Documentação atualizada em 30/12/2024*
