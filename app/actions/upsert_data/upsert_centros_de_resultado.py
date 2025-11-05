import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "CR_ID",
    "NOME",
    "DESCRICAO",
    "EMP_ID",
    "CR_ID_PAI",
    "USU_ID_GER",
    "USU_ID_ATD_SOL",
    "USU_ID_PAG_SOL",
    "USU_ID_APROV_DESPESAS",
    "CALC_ORDEM",
    "CALC_NIVEL",
    "CALC_HIERARQUIA",
    "IND_ATIVO",
    "IND_ADIANTAMENTOS",
    "INCLUIDO_EM",
    "ALTERADO_EM",
    "UDF1",
    "UDF2",
    "UDF3",
    "UDF4",
    "UDF5",
    "UDF6",
    "UDF7",
    "UDF8",
    "UDF9",
    "UDF10"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `CENTROS DE RESULTADO` (
    `CR_ID` INT,
    `NOME` VARCHAR(255),
    `DESCRICAO` TEXT,
    `EMP_ID` INT,
    `CR_ID_PAI` INT,
    `USU_ID_GER` INT,
    `USU_ID_ATD_SOL` INT,
    `USU_ID_PAG_SOL` INT,
    `USU_ID_APROV_DESPESAS` INT,
    `CALC_ORDEM` INT,
    `CALC_NIVEL` INT,
    `CALC_HIERARQUIA` DECIMAL(18,2),
    `IND_ATIVO` VARCHAR(1),
    `IND_ADIANTAMENTOS` VARCHAR(1),
    `INCLUIDO_EM` DATE,
    `ALTERADO_EM` DATE,
    `UDF1` VARCHAR(255),
    `UDF2` VARCHAR(255),
    `UDF3` VARCHAR(255),
    `UDF4` VARCHAR(255),
    `UDF5` VARCHAR(255),
    `UDF6` VARCHAR(255),
    `UDF7` VARCHAR(255),
    `UDF8` VARCHAR(255),
    `UDF9` VARCHAR(255),
    `UDF10` VARCHAR(255),

    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`CR_ID`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO CENTROS_DE_RESULTADO (
    CR_ID, NOME, DESCRICAO, EMP_ID, CR_ID_PAI, USU_ID_GER, USU_ID_ATD_SOL, USU_ID_PAG_SOL,
    USU_ID_APROV_DESPESAS, CALC_ORDEM, CALC_NIVEL, CALC_HIERARQUIA, IND_ATIVO, IND_ADIANTAMENTOS,
    INCLUIDO_EM, ALTERADO_EM, UDF1, UDF2, UDF3, UDF4, UDF5, UDF6, UDF7, UDF8, UDF9, UDF10
) VALUES (
    %(CR_ID)s, %(NOME)s, %(DESCRICAO)s, %(EMP_ID)s, %(CR_ID_PAI)s, %(USU_ID_GER)s, %(USU_ID_ATD_SOL)s, %(USU_ID_PAG_SOL)s,
    %(USU_ID_APROV_DESPESAS)s, %(CALC_ORDEM)s, %(CALC_NIVEL)s, %(CALC_HIERARQUIA)s, %(IND_ATIVO)s, %(IND_ADIANTAMENTOS)s,
    %(INCLUIDO_EM)s, %(ALTERADO_EM)s, %(UDF1)s, %(UDF2)s, %(UDF3)s, %(UDF4)s, %(UDF5)s, %(UDF6)s, %(UDF7)s, %(UDF8)s, %(UDF9)s, %(UDF10)s
)
ON DUPLICATE KEY UPDATE
    NOME = VALUES(NOME),
    DESCRICAO = VALUES(DESCRICAO),
    EMP_ID = VALUES(EMP_ID),
    CR_ID_PAI = VALUES(CR_ID_PAI),
    USU_ID_GER = VALUES(USU_ID_GER),
    USU_ID_ATD_SOL = VALUES(USU_ID_ATD_SOL),
    USU_ID_PAG_SOL = VALUES(USU_ID_PAG_SOL),
    USU_ID_APROV_DESPESAS = VALUES(USU_ID_APROV_DESPESAS),
    CALC_ORDEM = VALUES(CALC_ORDEM),
    CALC_NIVEL = VALUES(CALC_NIVEL),
    CALC_HIERARQUIA = VALUES(CALC_HIERARQUIA),
    IND_ATIVO = VALUES(IND_ATIVO),
    IND_ADIANTAMENTOS = VALUES(IND_ADIANTAMENTOS),
    INCLUIDO_EM = VALUES(INCLUIDO_EM),
    ALTERADO_EM = VALUES(ALTERADO_EM),
    UDF1 = VALUES(UDF1),
    UDF2 = VALUES(UDF2),
    UDF3 = VALUES(UDF3),
    UDF4 = VALUES(UDF4),
    UDF5 = VALUES(UDF5),
    UDF6 = VALUES(UDF6),
    UDF7 = VALUES(UDF7),
    UDF8 = VALUES(UDF8),
    UDF9 = VALUES(UDF9),
    UDF10 = VALUES(UDF10);
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
