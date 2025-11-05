import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "DESPT_ID",
    "CODIGO",
    "NOME",
    "DESCRICAO",
    "TIPO",
    "RGR_VALOR_MAXIMO",
    "IND_ATIVO",
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
    "TP_APONTAMENTO",
    "TAXA_ID_PGTO",
    "TAXA_ID_FAT"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `DESPESAS TIPO` (
    `DESPT_ID` INT,
    `CODIGO` VARCHAR(255),
    `NOME` VARCHAR(255),
    `DESCRICAO` TEXT,
    `TIPO` INT,
    `RGR_VALOR_MAXIMO` DECIMAL(18,2),
    `IND_ATIVO` VARCHAR(1),
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
    `TP_APONTAMENTO` INT,
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
INSERT INTO `DESPESAS TIPO` (
    DESPT_ID, CODIGO, NOME, DESCRICAO, TIPO, RGR_VALOR_MAXIMO, IND_ATIVO,
    INCLUIDO_EM, ALTERADO_EM, UDF1, UDF2, UDF3, UDF4, UDF5, UDF6, UDF7, UDF8, UDF9, UDF10,
    TP_APONTAMENTO, TAXA_ID_PGTO, TAXA_ID_FAT
) VALUES (
    %(DESPT_ID)s, %(CODIGO)s, %(NOME)s, %(DESCRICAO)s, %(TIPO)s, %(RGR_VALOR_MAXIMO)s, %(IND_ATIVO)s,
    %(INCLUIDO_EM)s, %(ALTERADO_EM)s, %(UDF1)s, %(UDF2)s, %(UDF3)s, %(UDF4)s, %(UDF5)s, %(UDF6)s, %(UDF7)s, %(UDF8)s, %(UDF9)s, %(UDF10)s,
    %(TP_APONTAMENTO)s, %(TAXA_ID_PGTO)s, %(TAXA_ID_FAT)s
)
ON DUPLICATE KEY UPDATE
    CODIGO = VALUES(CODIGO),
    NOME = VALUES(NOME),
    DESCRICAO = VALUES(DESCRICAO),
    TIPO = VALUES(TIPO),
    RGR_VALOR_MAXIMO = VALUES(RGR_VALOR_MAXIMO),
    IND_ATIVO = VALUES(IND_ATIVO),
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
    TP_APONTAMENTO = VALUES(TP_APONTAMENTO),
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
