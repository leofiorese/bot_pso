"""
Utilitários compartilhados para bulk upsert no PostgreSQL.
Substitui a lógica row-by-row duplicada nos 25 handlers.
"""
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
from db.db import get_conn, put_conn

# Registra adaptadores para que psycopg2 aceite tipos numpy nativamente
register_adapter(np.int64, lambda val: AsIs(int(val)))
register_adapter(np.int32, lambda val: AsIs(int(val)))
register_adapter(np.float64, lambda val: AsIs(float(val)))
register_adapter(np.float32, lambda val: AsIs(float(val)))
register_adapter(np.bool_, lambda val: AsIs(bool(val)))


def convert_date(value):
    """Converte string dd/mm/yyyy para yyyy-mm-dd (formato PostgreSQL DATE)."""
    if isinstance(value, str):
        try:
            return datetime.strptime(value, '%d/%m/%Y').strftime('%Y-%m-%d')
        except ValueError:
            return None
    return value


def clean_value(value):
    """Converte NaN/empty/None para None (NULL no PostgreSQL)."""
    if isinstance(value, float):
        if pd.isna(value):
            return None
    if value == "" or value is None:
        return None
    try:
        if pd.isnull(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def convert_yn_boolean(value):
    """Converte 'Y'/'N' para True/False (BOOLEAN nativo do PostgreSQL)."""
    if value == 'Y':
        return True
    elif value == 'N':
        return False
    return value


def apply_cleaning(df, date_columns=None, boolean_columns=None):
    """
    Aplica limpeza de dados no DataFrame.

    - date_columns: colunas com datas dd/mm/yyyy
    - boolean_columns: colunas com valores Y/N
    - Todas as colunas passam por clean_value (NaN -> None)
    """
    date_cols = set(date_columns or [])
    bool_cols = set(boolean_columns or [])

    for col in df.columns:
        if col in date_cols:
            df[col] = df[col].apply(convert_date)
        if col in bool_cols:
            df[col] = df[col].apply(convert_yn_boolean)
        df[col] = df[col].apply(clean_value)

    return df


def build_pg_upsert_sql(table_name, all_columns, pk_columns):
    """
    Gera SQL de upsert PostgreSQL dinamicamente.

    - table_name: nome da tabela (será convertido para lowercase)
    - all_columns: lista ordenada de colunas para INSERT
    - pk_columns: lista de colunas PK para ON CONFLICT

    Colunas PK são excluídas do DO UPDATE SET.
    created_at/updated_at são gerenciados por triggers do PG.
    """
    pk_set = set(pk_columns)
    exclude = {'created_at', 'updated_at'}

    update_columns = [c for c in all_columns if c not in pk_set and c not in exclude]

    q = lambda name: f'"{name}"'
    table = table_name.lower()

    col_list = ", ".join(q(c) for c in all_columns)
    conflict_cols = ", ".join(q(c) for c in pk_columns)
    update_set = ",\n        ".join(
        f'{q(c)} = EXCLUDED.{q(c)}' for c in update_columns
    )

    sql = f"""
    INSERT INTO {table} ({col_list})
    VALUES %s
    ON CONFLICT ({conflict_cols}) DO UPDATE SET
        {update_set}
    """
    return sql


def bulk_upsert(df, table_name, all_columns, pk_columns,
                date_columns=None, boolean_columns=None,
                csv_file_path=None, archive_func=None, archive_name=None,
                page_size=1000):
    """
    Executa bulk upsert de um DataFrame no PostgreSQL.

    Substitui o padrão df.iterrows() + cursor.execute() por execute_values().

    Args:
        df: DataFrame com dados
        table_name: nome da tabela destino
        all_columns: colunas ordenadas (mesma ordem do DataFrame)
        pk_columns: colunas da PK (para ON CONFLICT)
        date_columns: colunas dd/mm/yyyy para converter
        boolean_columns: colunas Y/N para converter
        csv_file_path: caminho do CSV (para arquivamento)
        archive_func: função(csv_path, nome) para arquivar
        archive_name: nome usado no arquivamento
        page_size: tamanho do lote para execute_values
    """
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()

        apply_cleaning(df, date_columns=date_columns, boolean_columns=boolean_columns)

        # Remove linhas duplicadas por PK (mantém a última ocorrência)
        # Necessário porque ON CONFLICT não aceita PKs duplicadas no mesmo batch
        df = df.drop_duplicates(subset=pk_columns, keep='last')

        upsert_sql = build_pg_upsert_sql(table_name, all_columns, pk_columns)

        data = []
        for _, row in df.iterrows():
            values = tuple(
                None if (isinstance(v, float) and pd.isna(v)) else v
                for v in (row[col] for col in all_columns)
            )
            data.append(values)

        if data:
            execute_values(cursor, upsert_sql, data, page_size=page_size)

        conn.commit()
        logging.info(f"Upsert realizado com sucesso: {table_name} ({len(data)} registros)")

        if csv_file_path and archive_func:
            archive_func(csv_file_path, archive_name or table_name)

    except Exception as e:
        logging.error(f"Erro no upsert para {table_name}: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            put_conn(conn)
