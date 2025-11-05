import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "FUNC_ID",
    "NOME",
    "DESCRICAO",
    "FUNC_ID_PAI",
    "TAXA_ID_FAT",
    "TAXA_ID_CUS",
    "ORDEM",
    "CALC_ORDEM",
    "CALC_NIVEL",
    "CALC_HIERARQUIA",
    "INCLUIDO_EM",
    "ALTERADO_EM"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `AGRUPAMENTO` (
    `FUNC_ID` INT,
    `NOME` VARCHAR(255),
    `DESCRICAO` TEXT,
    `FUNC_ID_PAI` INT,
    `TAXA_ID_FAT` DECIMAL(10,2),
    `TAXA_ID_CUS` DECIMAL(10,2),
    `ORDEM` DECIMAL(10,2),
    `CALC_ORDEM` INT,
    `CALC_NIVEL` INT,
    `CALC_HIERARQUIA` DECIMAL(10,2),
    `INCLUIDO_EM` DATE,
    `ALTERADO_EM` DATE,

    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`FUNC_ID`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO AGRUPAMENTO (
    FUNC_ID, NOME, DESCRICAO, FUNC_ID_PAI, TAXA_ID_FAT, TAXA_ID_CUS,
    ORDEM, CALC_ORDEM, CALC_NIVEL, CALC_HIERARQUIA, INCLUIDO_EM, ALTERADO_EM
) VALUES (
    %(FUNC_ID)s, %(NOME)s, %(DESCRICAO)s, %(FUNC_ID_PAI)s, %(TAXA_ID_FAT)s, %(TAXA_ID_CUS)s,
    %(ORDEM)s, %(CALC_ORDEM)s, %(CALC_NIVEL)s, %(CALC_HIERARQUIA)s, %(INCLUIDO_EM)s, %(ALTERADO_EM)s
)
ON DUPLICATE KEY UPDATE
    NOME = VALUES(NOME),
    DESCRICAO = VALUES(DESCRICAO),
    FUNC_ID_PAI = VALUES(FUNC_ID_PAI),
    TAXA_ID_FAT = VALUES(TAXA_ID_FAT),
    TAXA_ID_CUS = VALUES(TAXA_ID_CUS),
    ORDEM = VALUES(ORDEM),
    CALC_ORDEM = VALUES(CALC_ORDEM),
    CALC_NIVEL = VALUES(CALC_NIVEL),
    CALC_HIERARQUIA = VALUES(CALC_HIERARQUIA),
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
        logging.info(f"Substituindo valores NaN por None e formatando datas...")
        for col in df.columns:
            df[col] = df[col].apply(lambda x: clean_data(x, col))
        for _, row in df.iterrows():
            data_tuple = row.to_dict()
            for key, value in data_tuple.items():
                if isinstance(value, float) and pd.isna(value):
                    data_tuple[key] = None 
            cursor.execute(UPSERT_SQL, data_tuple)
        conn.commit()
        logging.info(f"Upsert realizado com sucesso na tabela {table_name}.")
        if os.path.exists(csv_file_path):
            os.remove(csv_file_path)
            logging.info(f"Arquivo CSV {csv_file_path} excluído com sucesso.")
    except Exception as e:
        logging.error(f"Erro no upsert: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
