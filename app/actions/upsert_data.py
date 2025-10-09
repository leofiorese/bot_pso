import logging
import pandas as pd
from app.db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "APON_ID","USU_ID", "ATIVO", "EMAIL", "NOME_USUARIO", "SIGLA", "TAXA_ID_CUS", "ATIV_ID",
    "DT_INICIO_APONTAMENTO", "MINUTOS", "PROJ_ID", "STATUS", "VALOR", "DT_EFETIVA",
    "B_DT_FIM_ATIVIDADE", "B_DT_INICIO_ATIVIDADE", "DT_FIM_ATIVIDADE", "DT_INICIO_ATIVIDADE",
    "DURACAO_PREVISTA", "IND_APO_BLOQUEADO", "IND_APROVADA", "IND_ENCERRADA",
    "TRABALHO_APONTADO_ATIVIDADE", "TRABALHO_FALTANDO_ATIVIDADE", "TRABALHO_PREVISTO_ATIVIDADE",
    "CODIGO_PROJETO", "DT_FIM_PROJETO", "DT_INICIO_PROJETO", "NOME_PROJETO",
    "TRABALHO_APONTADO_PROJETO", "TRABALHO_FALTANDO_PROJETO", "TRABALHO_PREVISTO_PROJETO",
    "TRABALHO_REALIZADO_PROJETO", "VALOR_PROJETO", "HORAS", "CUSTO_APONT"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `RELATORIO_PSO` (
    `APON_ID` INT,
    `USU_ID` INT,
    `ATIVO` BOOLEAN,
    `EMAIL` VARCHAR(255),
    `NOME_USUARIO` VARCHAR(255),
    `SIGLA` VARCHAR(10),
    `TAXA_ID_CUS` INT,
    `ATIV_ID` INT,
    `DT_INICIO_APONTAMENTO` DATETIME,
    `MINUTOS` INT,
    `PROJ_ID` INT,
    `STATUS` VARCHAR(5),
    `VALOR` DECIMAL(10, 2),
    `DT_EFETIVA` DATETIME,
    `B_DT_FIM_ATIVIDADE` DATETIME,
    `B_DT_INICIO_ATIVIDADE` DATETIME,
    `DT_FIM_ATIVIDADE` DATETIME,
    `DT_INICIO_ATIVIDADE` DATETIME,
    `DURACAO_PREVISTA` INT,
    `IND_APO_BLOQUEADO` BOOLEAN,
    `IND_APROVADA` BOOLEAN,
    `IND_ENCERRADA` BOOLEAN,
    `TRABALHO_APONTADO_ATIVIDADE` DECIMAL(10, 2),
    `TRABALHO_FALTANDO_ATIVIDADE` DECIMAL(10, 2),
    `TRABALHO_PREVISTO_ATIVIDADE` DECIMAL(10, 2),
    `CODIGO_PROJETO` VARCHAR(255),
    `DT_FIM_PROJETO` DATETIME,
    `DT_INICIO_PROJETO` DATETIME,
    `NOME_PROJETO` VARCHAR(255),
    `TRABALHO_APONTADO_PROJETO` DECIMAL(10, 2),
    `TRABALHO_FALTANDO_PROJETO` DECIMAL(10, 2),
    `TRABALHO_PREVISTO_PROJETO` DECIMAL(10, 2),
    `TRABALHO_REALIZADO_PROJETO` DECIMAL(10, 2),
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
  APON_ID, USU_ID, ATIVO, EMAIL, NOME_USUARIO, SIGLA, TAXA_ID_CUS, ATIV_ID,
  DT_INICIO_APONTAMENTO, MINUTOS, PROJ_ID, STATUS, VALOR, DT_EFETIVA,
  B_DT_FIM_ATIVIDADE, B_DT_INICIO_ATIVIDADE, DT_FIM_ATIVIDADE, DT_INICIO_ATIVIDADE,
  DURACAO_PREVISTA, IND_APO_BLOQUEADO, IND_APROVADA, IND_ENCERRADA,
  TRABALHO_APONTADO_ATIVIDADE, TRABALHO_FALTANDO_ATIVIDADE, TRABALHO_PREVISTO_ATIVIDADE,
  CODIGO_PROJETO, DT_FIM_PROJETO, DT_INICIO_PROJETO, NOME_PROJETO,
  TRABALHO_APONTADO_PROJETO, TRABALHO_FALTANDO_PROJETO, TRABALHO_PREVISTO_PROJETO,
  TRABALHO_REALIZADO_PROJETO, VALOR_PROJETO, HORAS, CUSTO_APONT
) VALUES (
  %(APON_ID)s, %(USU_ID)s, %(ATIVO)s, %(EMAIL)s, %(NOME_USUARIO)s, %(SIGLA)s, %(TAXA_ID_CUS)s, %(ATIV_ID)s,
  %(DT_INICIO_APONTAMENTO)s, %(MINUTOS)s, %(PROJ_ID)s, %(STATUS)s, %(VALOR)s, %(DT_EFETIVA)s,
  %(B_DT_FIM_ATIVIDADE)s, %(B_DT_INICIO_ATIVIDADE)s, %(DT_FIM_ATIVIDADE)s, %(DT_INICIO_ATIVIDADE)s,
  %(DURACAO_PREVISTA)s, %(IND_APO_BLOQUEADO)s, %(IND_APROVADA)s, %(IND_ENCERRADA)s,
  %(TRABALHO_APONTADO_ATIVIDADE)s, %(TRABALHO_FALTANDO_ATIVIDADE)s, %(TRABALHO_PREVISTO_ATIVIDADE)s,
  %(CODIGO_PROJETO)s, %(DT_FIM_PROJETO)s, %(DT_INICIO_PROJETO)s, %(NOME_PROJETO)s,
  %(TRABALHO_APONTADO_PROJETO)s, %(TRABALHO_FALTANDO_PROJETO)s, %(TRABALHO_PREVISTO_PROJETO)s,
  %(TRABALHO_REALIZADO_PROJETO)s, %(VALOR_PROJETO)s, %(HORAS)s, %(CUSTO_APONT)s
)
ON DUPLICATE KEY UPDATE
  USU_ID = VALUES(USU_ID),
  ATIVO = VALUES(ATIVO),
  EMAIL = VALUES(EMAIL),
  NOME_USUARIO = VALUES(NOME_USUARIO),
  SIGLA = VALUES(SIGLA),
  TAXA_ID_CUS = VALUES(TAXA_ID_CUS),
  ATIV_ID = VALUES(ATIV_ID),
  DT_INICIO_APONTAMENTO = VALUES(DT_INICIO_APONTAMENTO),
  MINUTOS = VALUES(MINUTOS),
  PROJ_ID = VALUES(PROJ_ID),
  STATUS = VALUES(STATUS),
  VALOR = VALUES(VALOR),
  DT_EFETIVA = VALUES(DT_EFETIVA),
  B_DT_FIM_ATIVIDADE = VALUES(B_DT_FIM_ATIVIDADE),
  B_DT_INICIO_ATIVIDADE = VALUES(B_DT_INICIO_ATIVIDADE),
  DT_FIM_ATIVIDADE = VALUES(DT_FIM_ATIVIDADE),
  DT_INICIO_ATIVIDADE = VALUES(DT_INICIO_ATIVIDADE),
  DURACAO_PREVISTA = VALUES(DURACAO_PREVISTA),
  IND_APO_BLOQUEADO = VALUES(IND_APO_BLOQUEADO),
  IND_APROVADA = VALUES(IND_APROVADA),
  IND_ENCERRADA = VALUES(IND_ENCERRADA),
  TRABALHO_APONTADO_ATIVIDADE = VALUES(TRABALHO_APONTADO_ATIVIDADE),
  TRABALHO_FALTANDO_ATIVIDADE = VALUES(TRABALHO_FALTANDO_ATIVIDADE),
  TRABALHO_PREVISTO_ATIVIDADE = VALUES(TRABALHO_PREVISTO_ATIVIDADE),
  CODIGO_PROJETO = VALUES(CODIGO_PROJETO),
  DT_FIM_PROJETO = VALUES(DT_FIM_PROJETO),
  DT_INICIO_PROJETO = VALUES(DT_INICIO_PROJETO),
  NOME_PROJETO = VALUES(NOME_PROJETO),
  TRABALHO_APONTADO_PROJETO = VALUES(TRABALHO_APONTADO_PROJETO),
  TRABALHO_FALTANDO_PROJETO = VALUES(TRABALHO_FALTANDO_PROJETO),
  TRABALHO_PREVISTO_PROJETO = VALUES(TRABALHO_PREVISTO_PROJETO),
  TRABALHO_REALIZADO_PROJETO = VALUES(TRABALHO_REALIZADO_PROJETO),
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
    
    if column_name in ["ATIVO", "IND_APO_BLOQUEADO", "IND_APROVADA", "IND_ENCERRADA"]:
        
        if value == 'Y':
            return True
        
        elif value == 'N':
            return False
    
    if column_name in ["DT_INICIO_APONTAMENTO", "DT_EFETIVA", "B_DT_FIM_ATIVIDADE", "B_DT_INICIO_ATIVIDADE", "DT_FIM_ATIVIDADE", "DT_INICIO_ATIVIDADE", "DT_FIM_PROJETO", "DT_INICIO_PROJETO"]:
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
            print(f"Arquivo CSV {csv_file_path} excluído com sucesso.")
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
