# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PSOffice Bot is a Python automation tool that extracts reports from the PSOffice web platform using Playwright, processes them with Pandas, and persists data to PostgreSQL (schema `psoffice`). It features a Tkinter GUI and supports 25 report types with automatic extraction and database synchronization.

## Development Setup

```bash
pip install -r requirements.txt
playwright install          # Firefox is used
playwright install-deps     # if system dependencies are missing
```

### Environment Configuration
A `.env` file in the project root is required:
```env
PSO_USERNAME=user@domain.com
PSO_PASSWORD=password
PSO_LOGIN_URL=https://psofficeapp.com.br/sandech/core/util/login.do?cdpy=...
PSO_REPORT_URL=https://psofficeapp.com.br/sandech/core/admin/usrep.do?...
HEADLESS=True
DB_HOST=localhost
DB_PORT=5432
DB_USER=pmo_admin
DB_PASSWORD=db_password
DB_NAME=pmo_hub
DB_POOL_MAX=5
DB_POOL_MIN=2
AUTO_CLOSE_ON_FINISH=True
```

### Running
```bash
python app/gui.py       # GUI entry point (primary)
python app/main.py      # Orchestrator without GUI
```

## Architecture

### Pipeline Per Report
Every report follows the same pipeline:
1. Login to PSOffice via Playwright (Firefox)
2. Fill textarea with MSSQL-syntax SQL query (`DATEADD`, `GETDATE()` — runs on PSOffice server, not the local DB)
3. Click "Testar (EXCEL)" to trigger download
4. Download lands as semicolon-delimited file in `app/downloads/`
5. `process_csv()` reads it with `pd.read_csv(delimiter=";", encoding="latin1")` and validates column count
6. `bulk_upsert()` inserts in bulk via `psycopg2.extras.execute_values()` with `ON CONFLICT DO UPDATE SET`
7. Archive CSV to network drive (or delete for Realizado/Orcado/Planejado)

### Strategy Pattern (main.py)
Two dicts dispatch report-specific logic:

- **`SCRIPT_GENERATORS`**: maps report name to SQL-generating function (`gerar_script_final(dateadd_string)`)
- **`UPSERT_HANDLERS`**: maps report name to lambda wrapping `upsert_data(df, table_name, csv_path)`

```python
SCRIPT_GENERATORS["AGRUPAMENTO"] = gerar_script_final_agrupamento
UPSERT_HANDLERS["AGRUPAMENTO"] = lambda df, csv: upsert_data_agrupamento(df, "AGRUPAMENTO", csv)
```

`DEFAULTS_30` maps all 25 keys to `"-500"` (days lookback when no custom date is set).

### Execution Modes

- **Automatic** (`user_choice=0`): Runs 22 reports sequentially (excludes Orcado, Planejado, Realizado). Triggered automatically after 10 seconds of GUI inactivity.
- **Manual** (`user_choice=1`): Runs a single user-selected report via GUI RadioButton.

Both modes: 3 retries with exponential backoff (`4s * attempt`), 5-second pause between reports.

### Shared State
`app/config_default_script.py` holds `script_choice_default = ""` as mutable global state. The GUI thread writes it; the worker thread reads it. No locking.

### Module Responsibilities

- **`app/main.py`** — Orchestration: login, report extraction, CSV processing, database upsert
- **`app/gui.py`** — Tkinter GUI: report selection, date config, log viewer (refreshes every 3s)
- **`app/sql_scripts/`** — 25 SQL query generators, one per report type
- **`app/actions/upsert_data/`** — 25 upsert handlers + `pg_upsert_utils.py` shared utility
- **`app/actions/process_csv/process_csv.py`** — CSV validation via `TABLE_MAP` dict
- **`app/db/db.py`** — PostgreSQL `SimpleConnectionPool` with `get_conn()`/`put_conn()`. Sets `search_path TO psoffice, public` on each connection checkout.
- **`app/utils.py`** — `arquivar_csv()` moves CSVs to hardcoded network path `Z:\3-Corporativo\PMO\...\BASE`

### Database Layer (`db.py`)
- **Connection Pool**: `psycopg2.pool.SimpleConnectionPool` (min=`DB_POOL_MIN`, max=`DB_POOL_MAX`)
- **Schema**: `SET search_path TO psoffice, public` — handlers use unqualified table names
- **Lifecycle**: Pool created lazily on first `get_conn()`, closed via `atexit` handler
- **Pattern**: `get_conn()` to obtain, `put_conn(conn)` to return (NOT `conn.close()`)

### Upsert Handler Structure
All handlers in `actions/upsert_data/` export:
```python
TABLE_COLUMNS = [...]          # Column name list (used by process_csv TABLE_MAP)
TABLE_NAME = "..."             # PostgreSQL table name (lowercase by search_path)
PK_COLUMNS = [...]             # Primary key column(s) for ON CONFLICT
DATE_COLUMNS = [...]           # Columns with dd/mm/yyyy dates to convert
BOOLEAN_COLUMNS = [...]        # Columns with Y/N values to convert to True/False
def upsert_data(df, table_name, csv_path): ...
```

Handlers delegate to `pg_upsert_utils.bulk_upsert()` which:
1. Cleans data (dates, booleans, NaN→None)
2. Builds `INSERT INTO ... ON CONFLICT (pk) DO UPDATE SET col = EXCLUDED.col`
3. Executes via `execute_values(cursor, sql, data, page_size=1000)`
4. Commits and archives/deletes CSV

Exception: `upsert_orcado_data.py`, `upsert_planejado_data.py`, `upsert_realizado_data.py` call `os.remove()` instead of `arquivar_csv()`.

### PostgreSQL Schema Notes
- All tables live in `psoffice` schema (handled by `search_path`)
- Column names are UPPERCASE and quoted with `"..."` in SQL
- `created_at`/`updated_at` are managed by PG triggers — never included in upsert
- Tables already exist in PostgreSQL; the bot does NOT create tables (no DDL)

## Adding New Report Types

5 registration points required:

1. **SQL Script** — Create `app/sql_scripts/new_report_script.py` with `gerar_script_final(dateadd_string)` function
2. **Upsert Handler** — Create `app/actions/upsert_data/upsert_new_report.py` with `TABLE_COLUMNS`, `TABLE_NAME`, `PK_COLUMNS`, `DATE_COLUMNS`, `BOOLEAN_COLUMNS`, and `upsert_data()` that calls `bulk_upsert()`
3. **main.py** — Add imports and register in both `SCRIPT_GENERATORS` and `UPSERT_HANDLERS` dicts
4. **process_csv.py** — Import `TABLE_COLUMNS` and add to `TABLE_MAP`
5. **gui.py** — Add RadioButton in `ask_for_script_choice()` popup (~line 100-150)

## Critical Technical Notes

### Playwright
- Firefox, 60s timeout on all interactions, headless controlled by `HEADLESS` env var
- Downloads go to `app/downloads/`
- 3 retries with exponential backoff (4s, 8s, 12s)

### SQL Injection Risk
`dateadd_string` is interpolated via f-strings in SQL scripts. Value comes from GUI input. Currently limited to controlled date formats, but be cautious when modifying date input handling.

### Network Drive Archiving
`arquivar_csv()` in `utils.py` uses hardcoded path `Z:\3-Corporativo\PMO\0-Gerência do PMO\0-Internos\04- PS Office\04. Relatório\BASE`. Not configurable via `.env`. Fails gracefully (logs error) if Z: drive is not mounted.

### Performance
- Bulk upsert via `execute_values()` with `page_size=1000` (replaces old row-by-row insertion)
- `SimpleConnectionPool` reuses connections (replaces old fresh-connection-per-call)
- Entire CSV loaded into DataFrame — may need chunking for very large reports

### Logging
File `pso_bot.log` in project root. INFO/ERROR levels. GUI log viewer refreshes every 3 seconds.

### Migration Reference
`MIGRATION_GUIDE_PYTHON_BOTS.md` documents the MySQL→PostgreSQL migration patterns used. `prd_migracao_postgresql.md` contains the PRD for this migration.
