# PRD: Migração do PSOffice Bot (MySQL → PostgreSQL)

## 1. Overview
- **Problem Statement:** A infraestrutura de dados do projeto (Hub PMO) foi consolidada de três bancos MySQL legados para um único banco de dados PostgreSQL 16 (contendo a divisão em 3 schemas). Sendo o PSOffice Bot o orquestrador principal de persistência das consultas de dados deste contexto, sua estrita operação em MySQL tornou-se incompatível e deve ser modernizada urgentemente para não travar a interoperabilidade sistêmica da companhia.
- **Objective:** Projetar e planejar detalhadamente a refatoração e migração direta das rotinas e conexões de persistência do PSOffice Bot da stack legada (`mysql-connector-python`) para a nova stack (`psycopg2-binary`); trocando lógicas defasadas *row-by-row* do MySQL por inserções massivas de alta performance permitidas pelo PostgreSQL. 
- **Stakeholders:** PMO (Gerência de Projetos) e Analistas que usam os painéis do Hub (com base nos dados populados pelo bot), Desenvolvedores e Engenheiros de Dados do Hub PMO.

## 2. Background & Context
- O bot contém 25 extratores e, subsequentemente, **25 funções que fazem rotinas de Upsert** manipulando DataFrames contra as tabelas de um banco localmente chamado no `.env`.
- Atualmente, as transações SQL utilizam sintaxes endêmicas do MySQL, como `INSERT ON DUPLICATE KEY UPDATE` e `REPLACE INTO`, o que causa bloqueios graves e falhas sintáticas no PostgreSQL.
- O mapeamento nativo de tipos mudou significativamente, requerendo uma maior rigidez, destacando-se a tratativa de booleanos, decimais e controle das rotinas nativas automáticas de trigger como `updated_at`.

## 3. Goals & Success Metrics
| Goal | Metric | Target |
|------|--------|--------|
| **Migração Total** | % de rotinas (`upsert_*.py`) migradas com sucesso | 100% dos 25 módulos. |
| **Performance Otimizada** | Tempo médio despendido apenas em insert SQL por relatório | Redução de pelo menos 80% (troca explícita por em bloco). |
| **Segurança Estrutural** | Taxa de falhas do ETL derivadas de tipagem SQL | 0% garantindo tratamentos de casting via Pandas. |

## 4. User Stories & Requirements

### Functional Requirements
- Como Desenvolvedor, eu desejo atualizar o gerenciador de dependências (`requirements.txt`) para contemplar o provedor de PostgreSQL nativo robusto.
- Como Mantenedor de Infraestrutura, eu desejo refatorar todos os modulos da pasta `app/actions/upsert_data/` para gerar queries utilizando `ON CONFLICT (pk) DO UPDATE SET...`.
- Como Arquiteto do Bot, eu desejo implementar a rotina utilitária `psycopg2.extras.execute_values` que possibilite o processamento contínuo das planilhas inteiras de forma paralela via tuplas, sem sobrecarga de cursor solitário num `for index, row in df.iterrows()`.

### Non-Functional Requirements
- **Performance / Eficiência:** Migrar da lógica transacional 1x1 baseada em single commit *rows-loops*, para chamando de *Bulk Upsert* enfileirada (`page_size=1000`).
- **Resiliência:** A nova subcamada de banco de dados (`db.py`) deverá possuir e administrar um `SimpleConnectionPool` limitando e guardando *idle connections* para não exceder limites de porta durante o ciclo automatizado do software com atrasos inerentes à instâncias web em paralelo.
- **Qualidade de Dados:** Validações de _casting_ em ambiente py (Decimais, Booleanos Nativos em lugar de Int) deverão ser sanitizados antes da passagem como Placeholder (%s).

## 5. Technical Architecture

- **Driver e Orquestração:** Substituição de `mysql.connector.connect()` pelo `psycopg2.pool.SimpleConnectionPool(...)`.
- **Topologia de Destino:**
  - Database: A variável correspondente à `DB_NAME` agora referirá a `pmo_hub`.
  - Prefixação Explícita: Todos as querries injetarão dados referenciando a extensão do esquema (Ex: `INSERT INTO psoffice.<tabela>`). 
  - Nomes de campos: Mudança de backticks (`` ` ``) para citações estritas duplas (`" "`), para previnir conflitos sintáticos e letras maiúsculas ou reservadas do Postgres.
- **Ciclo de Escrita (DML):**
  - **Antes ->** `DataFrame.iterrows()` -> loop disparando 2000 sentenças `CURSOR.EXECUTE()`.
  - **Depois ->** `DataFrame` vira `list[tuple()]` com tipagem convertida -> chamada única `execute_values(cursor, 'INSERT', tuples)` -> `conn.commit()`. 

## 6. Implementation Phases

### Phase 1: Troca de Drivers, Gerência e Controle no Core — [Semana 1]
- Eliminar legados do `requirements.txt` (`mysql-connector-python`, `pymysql`) e implementar `psycopg2-binary>=2.9.9`.
- Modificar o `.env` global para aceitar e empenhar `DB_PORT`, `DB_POOL_MAX` e `DB_POOL_MIN`.
- Refatorar a fábrica de conexão `app/db/db.py` adotando a interface e o descarregamento (`putconn`) de threads conectadas ao pool.
- Criar funções globais de adaptação (como um _helper_ casting function) localizadas em `process_csv.py` ou `utils.py` para limpeza massiva de campos.

### Phase 2: Refatoração Intensa e Upserts Handlers — [Semana 2]
- Abranger cada um dos 25 arquivos de relatórios mapeados nas categorias do sistema (Consolidados, Estrutura, Financeiro, Tempo...).
- Padronizar integralmente a troca das chaves:
  - De `ON DUPLICATE KEY UPDATE`
  - Para `ON CONFLICT (...) DO UPDATE SET...`.
- Implementar as chaves de conflito adequadas, focando nas tabelas que requerem Multi-Chave (`PKs compostas`, citadas no guia, como `relatorio_pso_orcado` e `relatorio_pso_planejado`).
- Aplicar o _execute_values_ de psycopg2 substituindo lógicas interativas de cursor do pandas.

### Phase 3: Sanitarização de Dicionário, DMLs e Ajustes Finais — [Semana 3]
- Mapear e retirar *queries* residuais de `TRUNCATE` (adicionando CASCADE caso necessário sob PSQL) e adaptar as chamadas que embutiam `REPLACE INTO`.
- Cuidado adicional na omissão da manipulação das colunas autogeridas, como `updated_at`, cuja responsabilidade de preenchimento recai agora sobre as triggers nativas do PostgreSQL estipuladas.

## 7. Risk Assessment
| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Falha sintática generalizada em colunas maiçusculas. | Alta | Elevado | Elaborar macros ou Regex em refactor para embutir aspas duplas de maneira consistente referindo os campos vindos dos arquivos do modelo de dados do PSOffice. |
| Percurso deficiente com Bool/Tipos | Média | Elevado | Modificar as variáveis contendo status ou ativo para garantir casting real como `True/False` literais do Python para as sentenças PSQL aceitarem o driver bind. |
| Gargalo logístico não intencional e deadlocks | Baixa | Médio | Testes rigorosos na instância homóloga assegurando requisições da classe Pool sendo encerradas e enviadas de volta corretamente após cada iteração concluída. |

## 8. Testing Strategy
- **Teste Unitário e Integração do DB Core:** Rodar scafolding isolados da classe pool certificando-se de que os timeouts estão funcionais e a autenticação se faz correta via URL e credenciais.
- **Piloto Seco de Extração:** Engatilhar o teste da inserção de um pacote leve (ex: Atividades ou Empresas) validando se todas as sintaxes de aspas e tipagem de data (`%d/%m/%Y` convertidas para tipo genérico de date Python) prosperarão.
- **Homologação Estressada:** Conduzir teste total iterativo percorrendo todo o ciclo em background sobre homologação para apurar se os tempos dos 25 arquivos são inferiores (em decorrência da alta massividade do Batching).

## 9. Rollout Plan
- Deployment incremental: Disponibilizar a build e gerar pacote isolado `.exe` de versão nova e testá-la numa máquina virtual (SandBox) antes de expô-la em Workstations de Produção.
- O plano prevê transição fluída: As migrações da estrutura SQL para a cloud já foram efetuadas. O Rollback baseia-se meramente na inversão do branch e do executável no usuário caso existam inconsistências reportadas em visualização de log.

## 10. Open Questions & Decisions
- [x] O PostgreSQL não criará tabelas (DDL dinâmico do robô antigo) com precisão. Caso a documentação exija, deverá ser feito alteração na rotina que gera as tabelas `CREATE TABLE IF NOT EXISTS` do Python atual adaptando a dialética dos tipos primários do PGSQL `SERIAL`, `NUMERIC`, etc., ou esta ação será abandonada se a nova arquitetura do Hub os forjou por completo? Resposta presumida: Se o novo DBA e o Hub dispuserem de uma fonte de schema `psoffice`, as flags de `CREATE...` dentro dos bots devem ser reavaliadas (ou ajustadas ao DDL PG).
- [ ] Houve confirmação com a área de rede sobre a nova porta e IP a ser liberado na VPN/WLAN local que comportam a porta `5432`? (Verificar caso timeout de TCP se torne uma constante).

## 11. Appendix
- MIGRATION_GUIDE_PYTHON_BOTS.md
- DOCUMENTACAO_PROJETO.md
