import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "PROJREC_ID",
    "PROJ_ID",
    "NOME",
    "DESCRICAO",
    "FUNC_ID",
    "TAXA_ID_FAT",
    "TAXA_ID_CUS",
    "TAXA_ID_PAG",
    "INCLUIDO_EM",
    "ALTERADO_EM",
    "TAXA_ID_CUS_PREV",
    "IND_AUTOMATICO"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `RECURSOS` (
    `PROJREC_ID` INT,
    `PROJ_ID` INT,
    `NOME` VARCHAR(255),
    `DESCRICAO` TEXT,
    `FUNC_ID` INT,
    `TAXA_ID_FAT` INT,
    `TAXA_ID_CUS` INT,
    `TAXA_ID_PAG` INT,
    `INCLUIDO_EM` DATE,
    `ALTERADO_EM` DATE,
    `TAXA_ID_CUS_PREV` INT,
    `IND_AUTOMATICO` VARCHAR(1),

    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`PROJREC_ID`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO RECURSOS (
    PROJREC_ID, PROJ_ID, NOME, DESCRICAO, FUNC_ID, TAXA_ID_FAT, TAXA_ID_CUS, TAXA_ID_PAG,
    INCLUIDO_EM, ALTERADO_EM, TAXA_ID_CUS_PREV, IND_AUTOMATICO
) VALUES (
    %(PROJREC_ID)s, %(PROJ_ID)s, %(NOME)s, %(DESCRICAO)s, %(FUNC_ID)s, %(TAXA_ID_FAT)s, %(TAXA_ID_CUS)s, %(TAXA_ID_PAG)s,
    %(INCLUIDO_EM)s, %(ALTERADO_EM)s, %(TAXA_ID_CUS_PREV)s, %(IND_AUTOMATICO)s
)
ON DUPLICATE KEY UPDATE
    PROJ_ID = VALUES(PROJ_ID),
    NOME = VALUES(NOME),
    DESCRICAO = VALUES(DESCRICAO),
    FUNC_ID = VALUES(FUNC_ID),
    TAXA_ID_FAT = VALUES(TAXA_ID_FAT),
    TAXA_ID_CUS = VALUES(TAXA_ID_CUS),
    TAXA_ID_PAG = VALUES(TAXA_ID_PAG),
    INCLUIDO_EM = VALUES(INCLUIDO_EM),
    ALTERADO_EM = VALUES(ALTERADO_EM),
    TAXA_ID_CUS_PREV = VALUES(TAXA_ID_CUS_PREV),
    IND_AUTOMATICO = VALUES(IND_AUTOMATICO);
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
    if column_name in ["INCLUIDO_EM", "ALTERADO_EM"]:
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
