import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "USU_ID",
    "FUNC_ID",
    "PERC_APROP",
    "DT_INCLUSAO",
    "DT_ALTERACAO"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `PSO_USU_FUNCOES` (
    `USU_ID` INT,
    `FUNC_ID` INT,
    `PERC_APROP` DECIMAL(10,2),
    `DT_INCLUSAO` DATE,
    `DT_ALTERACAO` DATE,

    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`USU_ID`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO PSO_USU_FUNCOES (
    USU_ID, FUNC_ID, PERC_APROP, DT_INCLUSAO, DT_ALTERACAO
) VALUES (
    %(USU_ID)s, %(FUNC_ID)s, %(PERC_APROP)s, %(DT_INCLUSAO)s, %(DT_ALTERACAO)s
)
ON DUPLICATE KEY UPDATE
    FUNC_ID = VALUES(FUNC_ID),
    PERC_APROP = VALUES(PERC_APROP),
    DT_INCLUSAO = VALUES(DT_INCLUSAO),
    DT_ALTERACAO = VALUES(DT_ALTERACAO);
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
    if column_name in ["DT_INCLUSAO", "DT_ALTERACAO"]:
        return convert_date(value)
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
