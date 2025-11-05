import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "ATRIB_ID",
    "ATIV_ID",
    "PROJREC_ID",
    "USU_ID",
    "FUNC_ID",
    "INDICE",
    "TRABALHO_PREVISTO",
    "TRABALHO_REALIZADO",
    "TRABALHO_APONTADO",
    "TRABALHO_FALTANDO",
    "B_TRABALHO_PREVISTO",
    "PERC_ALOCACAO",
    "ESTADO",
    "INCLUIDO_EM",
    "ALTERADO_EM",
    "IND_ENCERRADA"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `ATRIBUICOES` (
    `ATRIB_ID` INT,
    `ATIV_ID` INT,
    `PROJREC_ID` INT,
    `USU_ID` INT,
    `FUNC_ID` INT,
    `INDICE` DECIMAL(18,2),
    `TRABALHO_PREVISTO` DECIMAL(18,2),
    `TRABALHO_REALIZADO` DECIMAL(18,2),
    `TRABALHO_APONTADO` DECIMAL(18,2),
    `TRABALHO_FALTANDO` DECIMAL(18,2),
    `B_TRABALHO_PREVISTO` DECIMAL(18,2),
    `PERC_ALOCACAO` DECIMAL(10,2),
    `ESTADO` VARCHAR(100),
    `INCLUIDO_EM` DATE,
    `ALTERADO_EM` DATE,
    `IND_ENCERRADA` VARCHAR(1),

    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`ATRIB_ID`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO ATRIBUICOES (
    ATRIB_ID, ATIV_ID, PROJREC_ID, USU_ID, FUNC_ID, INDICE,
    TRABALHO_PREVISTO, TRABALHO_REALIZADO, TRABALHO_APONTADO, TRABALHO_FALTANDO,
    B_TRABALHO_PREVISTO, PERC_ALOCACAO, ESTADO, INCLUIDO_EM, ALTERADO_EM, IND_ENCERRADA
) VALUES (
    %(ATRIB_ID)s, %(ATIV_ID)s, %(PROJREC_ID)s, %(USU_ID)s, %(FUNC_ID)s, %(INDICE)s,
    %(TRABALHO_PREVISTO)s, %(TRABALHO_REALIZADO)s, %(TRABALHO_APONTADO)s, %(TRABALHO_FALTANDO)s,
    %(B_TRABALHO_PREVISTO)s, %(PERC_ALOCACAO)s, %(ESTADO)s, %(INCLUIDO_EM)s, %(ALTERADO_EM)s, %(IND_ENCERRADA)s
)
ON DUPLICATE KEY UPDATE
    ATIV_ID = VALUES(ATIV_ID),
    PROJREC_ID = VALUES(PROJREC_ID),
    USU_ID = VALUES(USU_ID),
    FUNC_ID = VALUES(FUNC_ID),
    INDICE = VALUES(INDICE),
    TRABALHO_PREVISTO = VALUES(TRABALHO_PREVISTO),
    TRABALHO_REALIZADO = VALUES(TRABALHO_REALIZADO),
    TRABALHO_APONTADO = VALUES(TRABALHO_APONTADO),
    TRABALHO_FALTANDO = VALUES(TRABALHO_FALTANDO),
    B_TRABALHO_PREVISTO = VALUES(B_TRABALHO_PREVISTO),
    PERC_ALOCACAO = VALUES(PERC_ALOCACAO),
    ESTADO = VALUES(ESTADO),
    INCLUIDO_EM = VALUES(INCLUIDO_EM),
    ALTERADO_EM = VALUES(ALTERADO_EM),
    IND_ENCERRADA = VALUES(IND_ENCERRADA);
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
