import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "RESHRATI_ID",
    "RESHR_ID",
    "ATRIB_ID",
    "DIA1",
    "DIA2",
    "DIA3",
    "DIA4",
    "DIA5",
    "DIA6",
    "DIA7",
    "DT_INCLUSAO",
    "DT_ALTERACAO"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `RESUMO_DE_HORAS_ATIV` (
    `RESHRATI_ID` INT,
    `RESHR_ID` INT,
    `ATRIB_ID` INT,
    `DIA1` INT,
    `DIA2` INT,
    `DIA3` INT,
    `DIA4` INT,
    `DIA5` INT,
    `DIA6` INT,
    `DIA7` INT,
    `DT_INCLUSAO` DATE,
    `DT_ALTERACAO` DATE,

    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`RESHRATI_ID`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO `RESUMO_DE_HORAS_ATIV` (
    RESHRATI_ID, RESHR_ID, ATRIB_ID, DIA1, DIA2, DIA3, DIA4, DIA5, DIA6, DIA7, DT_INCLUSAO, DT_ALTERACAO
) VALUES (
    %(RESHRATI_ID)s, %(RESHR_ID)s, %(ATRIB_ID)s, %(DIA1)s, %(DIA2)s, %(DIA3)s, %(DIA4)s, %(DIA5)s, %(DIA6)s, %(DIA7)s, %(DT_INCLUSAO)s, %(DT_ALTERACAO)s
)
ON DUPLICATE KEY UPDATE
    RESHR_ID = VALUES(RESHR_ID),
    ATRIB_ID = VALUES(ATRIB_ID),
    DIA1 = VALUES(DIA1),
    DIA2 = VALUES(DIA2),
    DIA3 = VALUES(DIA3),
    DIA4 = VALUES(DIA4),
    DIA5 = VALUES(DIA5),
    DIA6 = VALUES(DIA6),
    DIA7 = VALUES(DIA7),
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
