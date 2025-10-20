import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "CODIGO_PROJETO",
    "DT_FIM_PROJETO",
    "DT_INICIO_PROJETO",
    "NOME_PROJETO",
    "VALOR_PROJETO",
    "ATIVO",
    "PROJ_ID",
    "TRABALHO_APONTADO_PROJ",
    "TRABALHO_FALTANDO_PROJ",
    "TRABALHO_PREVISTO_PROJ",
    "TRABALHO_REALIZADO_PROJ",
    "DESCRICAO",
    "NOME_RECURSO",
    "TX_ID_RECURSO",
    "TX_RECURSO"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `RELATORIO_PSO_ORCADO` (
    `CODIGO_PROJETO` VARCHAR(255),
    `DT_FIM_PROJETO` DATE,
    `DT_INICIO_PROJETO` DATE,
    `NOME_PROJETO` VARCHAR(255),
    `VALOR_PROJETO` DECIMAL(20, 2),
    `ATIVO` BOOLEAN,
    `PROJ_ID` INT,
    `TRABALHO_APONTADO_PROJ` DECIMAL(10, 2),
    `TRABALHO_FALTANDO_PROJ` DECIMAL(10, 2),
    `TRABALHO_PREVISTO_PROJ` DECIMAL(10, 2),
    `TRABALHO_REALIZADO_PROJ` DECIMAL(10, 2),
    `DESCRICAO` TEXT,
    `NOME_RECURSO` VARCHAR(255),
    `TX_ID_RECURSO` INT,
    `TX_RECURSO` DECIMAL(10, 2),

    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`PROJ_ID`, `TX_ID_RECURSO`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO RELATORIO_PSO_ORCADO (
    CODIGO_PROJETO, DT_FIM_PROJETO, DT_INICIO_PROJETO, NOME_PROJETO, VALOR_PROJETO,
    ATIVO, PROJ_ID, TRABALHO_APONTADO_PROJ, TRABALHO_FALTANDO_PROJ,
    TRABALHO_PREVISTO_PROJ, TRABALHO_REALIZADO_PROJ, DESCRICAO, NOME_RECURSO,
    TX_ID_RECURSO, TX_RECURSO
) VALUES (
    %(CODIGO_PROJETO)s, %(DT_FIM_PROJETO)s, %(DT_INICIO_PROJETO)s, %(NOME_PROJETO)s,
    %(VALOR_PROJETO)s, %(ATIVO)s, %(PROJ_ID)s, %(TRABALHO_APONTADO_PROJ)s,
    %(TRABALHO_FALTANDO_PROJ)s, %(TRABALHO_PREVISTO_PROJ)s, %(TRABALHO_REALIZADO_PROJ)s,
    %(DESCRICAO)s, %(NOME_RECURSO)s, %(TX_ID_RECURSO)s, %(TX_RECURSO)s
)
ON DUPLICATE KEY UPDATE
    CODIGO_PROJETO = VALUES(CODIGO_PROJETO),
    DT_FIM_PROJETO = VALUES(DT_FIM_PROJETO),
    DT_INICIO_PROJETO = VALUES(DT_INICIO_PROJETO),
    NOME_PROJETO = VALUES(NOME_PROJETO),
    VALOR_PROJETO = VALUES(VALOR_PROJETO),
    ATIVO = VALUES(ATIVO),
    TRABALHO_APONTADO_PROJ = VALUES(TRABALHO_APONTADO_PROJ),
    TRABALHO_FALTANDO_PROJ = VALUES(TRABALHO_FALTANDO_PROJ),
    TRABALHO_PREVISTO_PROJ = VALUES(TRABALHO_PREVISTO_PROJ),
    TRABALHO_REALIZADO_PROJ = VALUES(TRABALHO_REALIZADO_PROJ),
    DESCRICAO = VALUES(DESCRICAO),
    NOME_RECURSO = VALUES(NOME_RECURSO),
    TX_RECURSO = VALUES(TX_RECURSO);
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
    
    if column_name in ["ATIVO"]:
        
        if value == 'Y':
            return True
        
        elif value == 'N':
            return False
    
    if column_name in ["DT_FIM_PROJETO", "DT_INICIO_PROJETO"]:
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
            logging.info(f"Coluna '{col}' processada.")

        for col in df.columns:
            if df[col].isnull().any():
                logging.warning(f"A coluna '{col}' contém valores nulos (NaN) que serão convertidos para NULL no banco.")

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
            logging.info("Cursor fechado.")
        
        if conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")
