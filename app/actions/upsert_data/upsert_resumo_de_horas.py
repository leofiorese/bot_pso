import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "RESHR_ID",
    "USU_ID",
    "DT_INICIO",
    "STATUS",
    "DIA1",
    "DIA2",
    "DIA3",
    "DIA4",
    "DIA5",
    "DIA6",
    "DIA7",
    "ATIV_ADDED",
    "INCLUIDO_EM",
    "ALTERADO_EM"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `RESUMO DE HORAS` (
    `RESHR_ID` INT,
    `USU_ID` INT,
    `DT_INICIO` DATE,
    `STATUS` VARCHAR(100),
    `DIA1` INT,
    `DIA2` INT,
    `DIA3` INT,
    `DIA4` INT,
    `DIA5` INT,
    `DIA6` INT,
    `DIA7` INT,
    `ATIV_ADDED` VARCHAR(10),
    `INCLUIDO_EM` DATE,
    `ALTERADO_EM` DATE,

    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`RESHR_ID`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO `RESUMO DE HORAS` (
    RESHR_ID, USU_ID, DT_INICIO, STATUS, DIA1, DIA2, DIA3, DIA4, DIA5, DIA6, DIA7, ATIV_ADDED, INCLUIDO_EM, ALTERADO_EM
) VALUES (
    %(RESHR_ID)s, %(USU_ID)s, %(DT_INICIO)s, %(STATUS)s, %(DIA1)s, %(DIA2)s, %(DIA3)s, %(DIA4)s, %(DIA5)s, %(DIA6)s, %(DIA7)s, %(ATIV_ADDED)s, %(INCLUIDO_EM)s, %(ALTERADO_EM)s
)
ON DUPLICATE KEY UPDATE
    USU_ID = VALUES(USU_ID),
    DT_INICIO = VALUES(DT_INICIO),
    STATUS = VALUES(STATUS),
    DIA1 = VALUES(DIA1),
    DIA2 = VALUES(DIA2),
    DIA3 = VALUES(DIA3),
    DIA4 = VALUES(DIA4),
    DIA5 = VALUES(DIA5),
    DIA6 = VALUES(DIA6),
    DIA7 = VALUES(DIA7),
    ATIV_ADDED = VALUES(ATIV_ADDED),
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
    if column_name in ["DT_INICIO", "INCLUIDO_EM", "ALTERADO_EM"]:
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
