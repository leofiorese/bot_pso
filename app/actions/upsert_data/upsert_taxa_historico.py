import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "TAXAH_ID",
    "TAXA_ID",
    "VALOR",
    "DT_EFETIVA",
    "LOOKUP",
    "INCLUIDO_EM",
    "ALTERADO_EM"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `TAXA_HISTORICO` (
    `TAXAH_ID` INT,
    `TAXA_ID` INT,
    `VALOR` DECIMAL(18,2),
    `DT_EFETIVA` DATE,
    `LOOKUP` TEXT,
    `INCLUIDO_EM` DATE,
    `ALTERADO_EM` DATE,

    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`TAXAH_ID`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO `TAXA_HISTORICO` (
    TAXAH_ID, TAXA_ID, VALOR, DT_EFETIVA, LOOKUP, INCLUIDO_EM, ALTERADO_EM
) VALUES (
    %(TAXAH_ID)s, %(TAXA_ID)s, %(VALOR)s, %(DT_EFETIVA)s, %(LOOKUP)s, %(INCLUIDO_EM)s, %(ALTERADO_EM)s
)
ON DUPLICATE KEY UPDATE
    TAXA_ID = VALUES(TAXA_ID),
    VALOR = VALUES(VALOR),
    DT_EFETIVA = VALUES(DT_EFETIVA),
    LOOKUP = VALUES(LOOKUP),
    INCLUIDO_EM = VALUES(INCLUIDO_EM),
    ALTERADO_EM = VALUES(ALTERADO_EM);
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
    if column_name in ["DT_EFETIVA", "INCLUIDO_EM", "ALTERADO_EM"]:
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
