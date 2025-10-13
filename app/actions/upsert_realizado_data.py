import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "APON_ID","USU_ID", "ATIVO", "EMAIL", "NOME_USUARIO", "ATIV_ID",
    "DT_INICIO_APONTAMENTO", "PROJ_ID", "TX_COLABORADOR", "NOME_ATIVIDADE",
    "B_DT_FIM_ATIVIDADE", "B_DT_INICIO_ATIVIDADE", "DT_FIM_ATIVIDADE", "DT_INICIO_ATIVIDADE",
    "DURACAO_PREVISTA", "TRABALHO_APONTADO_ATIVIDADE", "TRABALHO_FALTANDO_ATIVIDADE", "TRABALHO_PREVISTO_ATIVIDADE",
    "CODIGO_PROJETO", "DT_FIM_PROJETO", "DT_INICIO_PROJETO", "NOME_PROJETO", "VALOR_PROJETO", "HORAS", "CUSTO_APONT"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `RELATORIO_PSO` (
    `APON_ID` INT,
    `USU_ID` INT,
    `ATIVO` BOOLEAN,
    `EMAIL` VARCHAR(255),
    `NOME_USUARIO` VARCHAR(255),
    `ATIV_ID` INT,
    `DT_INICIO_APONTAMENTO` DATETIME,
    `PROJ_ID` INT,
    `TX_COLABORADOR` DECIMAL(10, 2),
    `NOME_ATIVIDADE` VARCHAR(255),
    `B_DT_FIM_ATIVIDADE` DATETIME,
    `B_DT_INICIO_ATIVIDADE` DATETIME,
    `DT_FIM_ATIVIDADE` DATETIME,
    `DT_INICIO_ATIVIDADE` DATETIME,
    `DURACAO_PREVISTA` INT,
    `TRABALHO_APONTADO_ATIVIDADE` DECIMAL(10, 2),
    `TRABALHO_FALTANDO_ATIVIDADE` DECIMAL(10, 2),
    `TRABALHO_PREVISTO_ATIVIDADE` DECIMAL(10, 2),
    `CODIGO_PROJETO` VARCHAR(255),
    `DT_FIM_PROJETO` DATETIME,
    `DT_INICIO_PROJETO` DATETIME,
    `NOME_PROJETO` VARCHAR(255),
    `VALOR_PROJETO` DECIMAL(20, 2),
    `HORAS` DECIMAL(10, 2),
    `CUSTO_APONT` DECIMAL(10, 2),

    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`APON_ID`)
) 

ENGINE=InnoDB 
DEFAULT CHARSET=utf8mb4 
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO RELATORIO_PSO (
  APON_ID, USU_ID, ATIVO, EMAIL, NOME_USUARIO, ATIV_ID,
  DT_INICIO_APONTAMENTO, PROJ_ID, TX_COLABORADOR, NOME_ATIVIDADE,
  B_DT_FIM_ATIVIDADE, B_DT_INICIO_ATIVIDADE, DT_FIM_ATIVIDADE, DT_INICIO_ATIVIDADE,
  DURACAO_PREVISTA,TRABALHO_APONTADO_ATIVIDADE, TRABALHO_FALTANDO_ATIVIDADE, TRABALHO_PREVISTO_ATIVIDADE,
  CODIGO_PROJETO, DT_FIM_PROJETO, DT_INICIO_PROJETO, NOME_PROJETO, VALOR_PROJETO, HORAS, CUSTO_APONT
) VALUES (
  %(APON_ID)s, %(USU_ID)s, %(ATIVO)s, %(EMAIL)s, %(NOME_USUARIO)s, %(ATIV_ID)s,
  %(DT_INICIO_APONTAMENTO)s, %(PROJ_ID)s, %(TX_COLABORADOR)s, %(NOME_ATIVIDADE)s,
  %(B_DT_FIM_ATIVIDADE)s, %(B_DT_INICIO_ATIVIDADE)s, %(DT_FIM_ATIVIDADE)s, %(DT_INICIO_ATIVIDADE)s,
  %(DURACAO_PREVISTA)s,%(TRABALHO_APONTADO_ATIVIDADE)s, %(TRABALHO_FALTANDO_ATIVIDADE)s, %(TRABALHO_PREVISTO_ATIVIDADE)s,
  %(CODIGO_PROJETO)s, %(DT_FIM_PROJETO)s, %(DT_INICIO_PROJETO)s, %(NOME_PROJETO)s, %(VALOR_PROJETO)s, %(HORAS)s, %(CUSTO_APONT)s
)
ON DUPLICATE KEY UPDATE
  USU_ID = VALUES(USU_ID),
  ATIVO = VALUES(ATIVO),
  EMAIL = VALUES(EMAIL),
  NOME_USUARIO = VALUES(NOME_USUARIO),
  ATIV_ID = VALUES(ATIV_ID),
  DT_INICIO_APONTAMENTO = VALUES(DT_INICIO_APONTAMENTO),
  PROJ_ID = VALUES(PROJ_ID),
  TX_COLABORADOR = VALUES(TX_COLABORADOR),
  NOME_ATIVIDADE = VALUES(NOME_ATIVIDADE),
  B_DT_FIM_ATIVIDADE = VALUES(B_DT_FIM_ATIVIDADE),
  B_DT_INICIO_ATIVIDADE = VALUES(B_DT_INICIO_ATIVIDADE),
  DT_FIM_ATIVIDADE = VALUES(DT_FIM_ATIVIDADE),
  DT_INICIO_ATIVIDADE = VALUES(DT_INICIO_ATIVIDADE),
  DURACAO_PREVISTA = VALUES(DURACAO_PREVISTA),
  TRABALHO_APONTADO_ATIVIDADE = VALUES(TRABALHO_APONTADO_ATIVIDADE),
  TRABALHO_FALTANDO_ATIVIDADE = VALUES(TRABALHO_FALTANDO_ATIVIDADE),
  TRABALHO_PREVISTO_ATIVIDADE = VALUES(TRABALHO_PREVISTO_ATIVIDADE),
  CODIGO_PROJETO = VALUES(CODIGO_PROJETO),
  DT_FIM_PROJETO = VALUES(DT_FIM_PROJETO),
  DT_INICIO_PROJETO = VALUES(DT_INICIO_PROJETO),
  NOME_PROJETO = VALUES(NOME_PROJETO),
  VALOR_PROJETO = VALUES(VALOR_PROJETO),
  HORAS = VALUES(HORAS),
  CUSTO_APONT = VALUES(CUSTO_APONT)
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
    
    if column_name in ["DT_INICIO_APONTAMENTO", "B_DT_FIM_ATIVIDADE", "B_DT_INICIO_ATIVIDADE", "DT_FIM_ATIVIDADE", "DT_INICIO_ATIVIDADE", "DT_FIM_PROJETO", "DT_INICIO_PROJETO"]:
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
