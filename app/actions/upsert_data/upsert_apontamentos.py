import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "APON_ID",
    "RESHRATI_ID",
    "USU_ID",
    "RESHR_ID",
    "ATRIB_ID",
    "PROJREC_ID",
    "ATIV_ID",
    "PROJ_ID",
    "DT_INICIO",
    "HR_INICIO",
    "MINUTOS",
    "MINUTOS_FALTANDO",
    "MINUTOS_FATURADOS",
    "MINUTOS_RECONHECIDOS",
    "MINUTOS_FAT_EXT",
    "MINUTOS_FAT_EXT_AD",
    "MINUTOS_REC_EXT",
    "MINUTOS_REC_EXT_AD",
    "FAT_ID",
    "PAG_ID",
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
    "UDF10",
    "DET1",
    "AUX1",
    "INTR1",
    "INTR2",
    "INTR3",
    "INTR4",
    "INTR5",
    "WKLOG_JIRA_ID",
    "DT_SUBMISSAO",
    "DT_APROVACAO_1",
    "DT_APROVACAO_2"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `APONTAMENTOS` (
    `APON_ID` INT,
    `RESHRATI_ID` INT,
    `USU_ID` INT,
    `RESHR_ID` INT,
    `ATRIB_ID` INT,
    `PROJREC_ID` INT,
    `ATIV_ID` INT,
    `PROJ_ID` INT,
    `DT_INICIO` DATE,
    `HR_INICIO` DATE,
    `MINUTOS` INT,
    `MINUTOS_FALTANDO` INT,
    `MINUTOS_FATURADOS` INT,
    `MINUTOS_RECONHECIDOS` INT,
    `MINUTOS_FAT_EXT` INT,
    `MINUTOS_FAT_EXT_AD` INT,
    `MINUTOS_REC_EXT` INT,
    `MINUTOS_REC_EXT_AD` INT,
    `FAT_ID` INT,
    `PAG_ID` INT,
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
    `DET1` DECIMAL(18,2),
    `AUX1` DECIMAL(18,2),
    `INTR1` DECIMAL(18,2),
    `INTR2` DECIMAL(18,2),
    `INTR3` DECIMAL(18,2),
    `INTR4` DECIMAL(18,2),
    `INTR5` DECIMAL(18,2),
    `WKLOG_JIRA_ID` DECIMAL(18,2),
    `DT_SUBMISSAO` DATE,
    `DT_APROVACAO_1` DATE,
    `DT_APROVACAO_2` DATE,

    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`APON_ID`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO APONTAMENTOS (
    APON_ID, RESHRATI_ID, USU_ID, RESHR_ID, ATRIB_ID, PROJREC_ID, ATIV_ID, PROJ_ID,
    DT_INICIO, HR_INICIO, MINUTOS, MINUTOS_FALTANDO, MINUTOS_FATURADOS, MINUTOS_RECONHECIDOS,
    MINUTOS_FAT_EXT, MINUTOS_FAT_EXT_AD, MINUTOS_REC_EXT, MINUTOS_REC_EXT_AD, FAT_ID, PAG_ID,
    INCLUIDO_EM, ALTERADO_EM, UDF1, UDF2, UDF3, UDF4, UDF5, UDF6, UDF7, UDF8, UDF9, UDF10,
    DET1, AUX1, INTR1, INTR2, INTR3, INTR4, INTR5, WKLOG_JIRA_ID, DT_SUBMISSAO, DT_APROVACAO_1, DT_APROVACAO_2
) VALUES (
    %(APON_ID)s, %(RESHRATI_ID)s, %(USU_ID)s, %(RESHR_ID)s, %(ATRIB_ID)s, %(PROJREC_ID)s, %(ATIV_ID)s, %(PROJ_ID)s,
    %(DT_INICIO)s, %(HR_INICIO)s, %(MINUTOS)s, %(MINUTOS_FALTANDO)s, %(MINUTOS_FATURADOS)s, %(MINUTOS_RECONHECIDOS)s,
    %(MINUTOS_FAT_EXT)s, %(MINUTOS_FAT_EXT_AD)s, %(MINUTOS_REC_EXT)s, %(MINUTOS_REC_EXT_AD)s, %(FAT_ID)s, %(PAG_ID)s,
    %(INCLUIDO_EM)s, %(ALTERADO_EM)s, %(UDF1)s, %(UDF2)s, %(UDF3)s, %(UDF4)s, %(UDF5)s, %(UDF6)s, %(UDF7)s, %(UDF8)s, %(UDF9)s, %(UDF10)s,
    %(DET1)s, %(AUX1)s, %(INTR1)s, %(INTR2)s, %(INTR3)s, %(INTR4)s, %(INTR5)s, %(WKLOG_JIRA_ID)s, %(DT_SUBMISSAO)s, %(DT_APROVACAO_1)s, %(DT_APROVACAO_2)s
)
ON DUPLICATE KEY UPDATE
    RESHRATI_ID = VALUES(RESHRATI_ID),
    USU_ID = VALUES(USU_ID),
    RESHR_ID = VALUES(RESHR_ID),
    ATRIB_ID = VALUES(ATRIB_ID),
    PROJREC_ID = VALUES(PROJREC_ID),
    ATIV_ID = VALUES(ATIV_ID),
    PROJ_ID = VALUES(PROJ_ID),
    DT_INICIO = VALUES(DT_INICIO),
    HR_INICIO = VALUES(HR_INICIO),
    MINUTOS = VALUES(MINUTOS),
    MINUTOS_FALTANDO = VALUES(MINUTOS_FALTANDO),
    MINUTOS_FATURADOS = VALUES(MINUTOS_FATURADOS),
    MINUTOS_RECONHECIDOS = VALUES(MINUTOS_RECONHECIDOS),
    MINUTOS_FAT_EXT = VALUES(MINUTOS_FAT_EXT),
    MINUTOS_FAT_EXT_AD = VALUES(MINUTOS_FAT_EXT_AD),
    MINUTOS_REC_EXT = VALUES(MINUTOS_REC_EXT),
    MINUTOS_REC_EXT_AD = VALUES(MINUTOS_REC_EXT_AD),
    FAT_ID = VALUES(FAT_ID),
    PAG_ID = VALUES(PAG_ID),
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
    UDF10 = VALUES(UDF10),
    DET1 = VALUES(DET1),
    AUX1 = VALUES(AUX1),
    INTR1 = VALUES(INTR1),
    INTR2 = VALUES(INTR2),
    INTR3 = VALUES(INTR3),
    INTR4 = VALUES(INTR4),
    INTR5 = VALUES(INTR5),
    WKLOG_JIRA_ID = VALUES(WKLOG_JIRA_ID),
    DT_SUBMISSAO = VALUES(DT_SUBMISSAO),
    DT_APROVACAO_1 = VALUES(DT_APROVACAO_1),
    DT_APROVACAO_2 = VALUES(DT_APROVACAO_2);
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
    if column_name in [
        "DT_INICIO","HR_INICIO","INCLUIDO_EM","ALTERADO_EM",
        "DT_SUBMISSAO","DT_APROVACAO_1","DT_APROVACAO_2"
    ]:
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
