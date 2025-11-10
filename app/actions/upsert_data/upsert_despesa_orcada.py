import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "PROJ_ID",
    "DESPT_ID",
    "REEMBOLSAVEL",
    "COBRAVEL",
    "RGR_VALOR_MAXIMO",
    "VALOR_PREVISTO",
    "INCLUIDO_EM",
    "ALTERADO_EM",
    "LIMITE_APONTAMENTO",
    "LIMITE_MAXIMO",
    "APON_BLOQUEADO",
    "TAXA_ID_PGTO",
    "TAXA_ID_FAT"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `DESPESA_ORCADA` (
    `PROJ_ID` INT,
    `DESPT_ID` INT,
    `REEMBOLSAVEL` BOOLEAN,
    `COBRAVEL` BOOLEAN,
    `RGR_VALOR_MAXIMO` VARCHAR(255),
    `VALOR_PREVISTO` DECIMAL(18,2),
    `INCLUIDO_EM` DATE,
    `ALTERADO_EM` DATE,
    `LIMITE_APONTAMENTO` DECIMAL(18,2),
    `LIMITE_MAXIMO` DECIMAL(18,2),
    `APON_BLOQUEADO` BOOLEAN,
    `TAXA_ID_PGTO` INT,
    `TAXA_ID_FAT` INT,

    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`DESPT_ID`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO `DESPESA_ORCADA` (
    PROJ_ID, DESPT_ID, REEMBOLSAVEL, COBRAVEL, RGR_VALOR_MAXIMO, VALOR_PREVISTO,
    INCLUIDO_EM, ALTERADO_EM, LIMITE_APONTAMENTO, LIMITE_MAXIMO, APON_BLOQUEADO,
    TAXA_ID_PGTO, TAXA_ID_FAT
) VALUES (
    %(PROJ_ID)s, %(DESPT_ID)s, %(REEMBOLSAVEL)s, %(COBRAVEL)s, %(RGR_VALOR_MAXIMO)s, %(VALOR_PREVISTO)s,
    %(INCLUIDO_EM)s, %(ALTERADO_EM)s, %(LIMITE_APONTAMENTO)s, %(LIMITE_MAXIMO)s, %(APON_BLOQUEADO)s,
    %(TAXA_ID_PGTO)s, %(TAXA_ID_FAT)s
)
ON DUPLICATE KEY UPDATE
    PROJ_ID = VALUES(PROJ_ID),
    REEMBOLSAVEL = VALUES(REEMBOLSAVEL),
    COBRAVEL = VALUES(COBRAVEL),
    RGR_VALOR_MAXIMO = VALUES(RGR_VALOR_MAXIMO),
    VALOR_PREVISTO = VALUES(VALOR_PREVISTO),
    INCLUIDO_EM = VALUES(INCLUIDO_EM),
    ALTERADO_EM = VALUES(ALTERADO_EM),
    LIMITE_APONTAMENTO = VALUES(LIMITE_APONTAMENTO),
    LIMITE_MAXIMO = VALUES(LIMITE_MAXIMO),
    APON_BLOQUEADO = VALUES(APON_BLOQUEADO),
    TAXA_ID_PGTO = VALUES(TAXA_ID_PGTO),
    TAXA_ID_FAT = VALUES(TAXA_ID_FAT);
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

    if column_name in ["REEMBOLSAVEL", "COBRAVEL", "APON_BLOQUEADO"]:
        if value == 'Y':
            return True
        elif value == 'N':
            return False

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
