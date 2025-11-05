import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "CAL_ID",
    "NOME",
    "IS_DEFAULT"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `CALENDARIOS` (
    `CAL_ID` INT,
    `NOME` VARCHAR(255),
    `IS_DEFAULT` VARCHAR(1),

    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`CAL_ID`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO CALENDARIOS (
    CAL_ID, NOME, IS_DEFAULT
) VALUES (
    %(CAL_ID)s, %(NOME)s, %(IS_DEFAULT)s
)
ON DUPLICATE KEY UPDATE
    NOME = VALUES(NOME),
    IS_DEFAULT = VALUES(IS_DEFAULT);
"""

def create_table(cursor, table_name):
    try:
        cursor.execute(CREATE_TABLE_SQL)
        logging.info(f"Tabela {table_name} criada/verificada.")
    except Exception as e:
        logging.error(f"Erro ao criar/verificar a tabela: {e}")
        raise

def convert_date(value):
    if isinstance(value, str):
        try:
            return datetime.strptime(value, '%d/%m/%Y').strftime('%Y-%m-%d')
        except ValueError:
            return None  
    return value

def clean_data(value, column_name):
    if pd.isna(value) or value == "" or value is None or pd.isnull(value):
        return None
    return value

def upsert_data(df: pd.DataFrame, table_name: str, csv_file_path: str):
    conn = None
    cursor = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        create_table(cursor, table_name)
        for col in df.columns:
            df[col] = df[col].apply(lambda x: clean_data(x, col))
        for _, row in df.iterrows():
            data_tuple = row.to_dict()
            for key, value in data_tuple.items():
                if isinstance(value, float) and pd.isna(value):
                    data_tuple[key] = None 
            cursor.execute(UPSERT_SQL, data_tuple)
        conn.commit()
        if os.path.exists(csv_file_path):
            os.remove(csv_file_path)
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
