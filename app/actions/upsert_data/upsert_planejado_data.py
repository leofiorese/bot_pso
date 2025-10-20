import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    'CODIGO_PROJETO',
    'DT_FIM_PROJETO',
    'DT_INICIO_PROJETO',
    'NOME_PROJETO',
    'VALOR_PROJETO',
    'ATIVO',
    'PROJ_ID',
    'DT_CALCULO',
    'VALOR_CUSTO_TOTAL_ORCADO_HORAS',
    'VALOR_CUSTO_TOTAL_REAL_HORAS',
    'VALOR_CUSTO_TOTAL_FALTANDO_HORAS',
    'VALOR_CUSTO_TOTAL_ORCADO_DESPESAS',
    'VALOR_CUSTO_TOTAL_REAL_DESPESAS',
    'VALOR_CUSTO_TOTAL_FALTANDO_DESPESAS',
    'ATIV_ID',
    'NOME_ATIVIDADE',
    'B_DT_FIM_ATIVIDADE',
    'B_DT_INICIO_ATIVIDADE',
    'DT_FIM_ATIVIDADE',
    'DT_INICIO_ATIVIDADE',
    'DURACAO_PREVISTA_HORAS',
    'TRABALHO_TOTAL_APONTADO_ATIVIDADE',
    'TRABALHO_TOTAL_FALTANDO_ATIVIDADE',
    'TRABALHO_TOTAL_PREVISTO_ATIVIDADE',
    'ATRIB_ID',
    'PROJREC_ID',
    'USU_ID',
    'TRABALHO_RECURSO_APONTADO_ATIVIDADE',
    'TRABALHO_RECURSO_FALTANDO_ATIVIDADE',
    'TRABALHO_RECURSO_PREVISTO_ATIVIDADE',
    'PERC_ALOCACAO',
    'NOME_RECURSO',
    'DESCRICAO',
    'TX_ID_RECURSO'
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `RELATORIO_PSO_PLANEJADO` (
    `CODIGO_PROJETO` VARCHAR(255),
    `DT_FIM_PROJETO` DATETIME,
    `DT_INICIO_PROJETO` DATETIME,
    `NOME_PROJETO` VARCHAR(255),
    `VALOR_PROJETO` DECIMAL(15, 2),
    `ATIVO` BOOLEAN,
    `PROJ_ID` INT NOT NULL,
    `DT_CALCULO` DATETIME,
    `VALOR_CUSTO_TOTAL_ORCADO_HORAS` DECIMAL(15, 2),
    `VALOR_CUSTO_TOTAL_REAL_HORAS` DECIMAL(15, 2),
    `VALOR_CUSTO_TOTAL_FALTANDO_HORAS` DECIMAL(15, 2),
    `VALOR_CUSTO_TOTAL_ORCADO_DESPESAS` DECIMAL(15, 2),
    `VALOR_CUSTO_TOTAL_REAL_DESPESAS` DECIMAL(15, 2),
    `VALOR_CUSTO_TOTAL_FALTANDO_DESPESAS` DECIMAL(15, 2),
    `ATIV_ID` INT NOT NULL,
    `NOME_ATIVIDADE` VARCHAR(255),
    `B_DT_FIM_ATIVIDADE` DATETIME,
    `B_DT_INICIO_ATIVIDADE` DATETIME,
    `DT_FIM_ATIVIDADE` DATETIME,
    `DT_INICIO_ATIVIDADE` DATETIME,
    `DURACAO_PREVISTA_HORAS` DECIMAL(10, 2),
    `TRABALHO_TOTAL_APONTADO_ATIVIDADE` DECIMAL(10, 2),
    `TRABALHO_TOTAL_FALTANDO_ATIVIDADE` DECIMAL(10, 2),
    `TRABALHO_TOTAL_PREVISTO_ATIVIDADE` DECIMAL(10, 2),
    `ATRIB_ID` INT NOT NULL,
    `PROJREC_ID` INT,
    `USU_ID` INT,
    `TRABALHO_RECURSO_APONTADO_ATIVIDADE` DECIMAL(10, 2),
    `TRABALHO_RECURSO_FALTANDO_ATIVIDADE` DECIMAL(10, 2),
    `TRABALHO_RECURSO_PREVISTO_ATIVIDADE` DECIMAL(10, 2),
    `PERC_ALOCACAO` DECIMAL(10, 2),
    `NOME_RECURSO` VARCHAR(255),
    `DESCRICAO` VARCHAR(4000),
    `TX_ID_RECURSO` INT,
    
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`PROJ_ID`, `ATIV_ID`, `ATRIB_ID`)
) 
ENGINE=InnoDB 
DEFAULT CHARSET=utf8mb4 
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO `RELATORIO_PSO_PLANEJADO` (
    `CODIGO_PROJETO`, `DT_FIM_PROJETO`, `DT_INICIO_PROJETO`, `NOME_PROJETO`, `VALOR_PROJETO`, 
    `ATIVO`, `PROJ_ID`, `DT_CALCULO`, `VALOR_CUSTO_TOTAL_ORCADO_HORAS`, `VALOR_CUSTO_TOTAL_REAL_HORAS`, 
    `VALOR_CUSTO_TOTAL_FALTANDO_HORAS`, `VALOR_CUSTO_TOTAL_ORCADO_DESPESAS`, 
    `VALOR_CUSTO_TOTAL_REAL_DESPESAS`, `VALOR_CUSTO_TOTAL_FALTANDO_DESPESAS`, `ATIV_ID`, 
    `NOME_ATIVIDADE`, `B_DT_FIM_ATIVIDADE`, `B_DT_INICIO_ATIVIDADE`, `DT_FIM_ATIVIDADE`, 
    `DT_INICIO_ATIVIDADE`, `DURACAO_PREVISTA_HORAS`, `TRABALHO_TOTAL_APONTADO_ATIVIDADE`, 
    `TRABALHO_TOTAL_FALTANDO_ATIVIDADE`, `TRABALHO_TOTAL_PREVISTO_ATIVIDADE`, `ATRIB_ID`, 
    `PROJREC_ID`, `USU_ID`, `TRABALHO_RECURSO_APONTADO_ATIVIDADE`, `TRABALHO_RECURSO_FALTANDO_ATIVIDADE`, 
    `TRABALHO_RECURSO_PREVISTO_ATIVIDADE`, `PERC_ALOCACAO`, `NOME_RECURSO`, `DESCRICAO`, `TX_ID_RECURSO`
) VALUES (
    %(CODIGO_PROJETO)s, %(DT_FIM_PROJETO)s, %(DT_INICIO_PROJETO)s, %(NOME_PROJETO)s, %(VALOR_PROJETO)s, 
    %(ATIVO)s, %(PROJ_ID)s, %(DT_CALCULO)s, %(VALOR_CUSTO_TOTAL_ORCADO_HORAS)s, %(VALOR_CUSTO_TOTAL_REAL_HORAS)s, 
    %(VALOR_CUSTO_TOTAL_FALTANDO_HORAS)s, %(VALOR_CUSTO_TOTAL_ORCADO_DESPESAS)s, 
    %(VALOR_CUSTO_TOTAL_REAL_DESPESAS)s, %(VALOR_CUSTO_TOTAL_FALTANDO_DESPESAS)s, %(ATIV_ID)s, 
    %(NOME_ATIVIDADE)s, %(B_DT_FIM_ATIVIDADE)s, %(B_DT_INICIO_ATIVIDADE)s, %(DT_FIM_ATIVIDADE)s, 
    %(DT_INICIO_ATIVIDADE)s, %(DURACAO_PREVISTA_HORAS)s, %(TRABALHO_TOTAL_APONTADO_ATIVIDADE)s, 
    %(TRABALHO_TOTAL_FALTANDO_ATIVIDADE)s, %(TRABALHO_TOTAL_PREVISTO_ATIVIDADE)s, %(ATRIB_ID)s, 
    %(PROJREC_ID)s, %(USU_ID)s, %(TRABALHO_RECURSO_APONTADO_ATIVIDADE)s, %(TRABALHO_RECURSO_FALTANDO_ATIVIDADE)s, 
    %(TRABALHO_RECURSO_PREVISTO_ATIVIDADE)s, %(PERC_ALOCACAO)s, %(NOME_RECURSO)s, %(DESCRICAO)s, %(TX_ID_RECURSO)s
)
ON DUPLICATE KEY UPDATE
    `CODIGO_PROJETO` = VALUES(`CODIGO_PROJETO`),
    `DT_FIM_PROJETO` = VALUES(`DT_FIM_PROJETO`),
    `DT_INICIO_PROJETO` = VALUES(`DT_INICIO_PROJETO`),
    `NOME_PROJETO` = VALUES(`NOME_PROJETO`),
    `VALOR_PROJETO` = VALUES(`VALOR_PROJETO`),
    `ATIVO` = VALUES(`ATIVO`),
    `DT_CALCULO` = VALUES(`DT_CALCULO`),
    `VALOR_CUSTO_TOTAL_ORCADO_HORAS` = VALUES(`VALOR_CUSTO_TOTAL_ORCADO_HORAS`),
    `VALOR_CUSTO_TOTAL_REAL_HORAS` = VALUES(`VALOR_CUSTO_TOTAL_REAL_HORAS`),
    `VALOR_CUSTO_TOTAL_FALTANDO_HORAS` = VALUES(`VALOR_CUSTO_TOTAL_FALTANDO_HORAS`),
    `VALOR_CUSTO_TOTAL_ORCADO_DESPESAS` = VALUES(`VALOR_CUSTO_TOTAL_ORCADO_DESPESAS`),
    `VALOR_CUSTO_TOTAL_REAL_DESPESAS` = VALUES(`VALOR_CUSTO_TOTAL_REAL_DESPESAS`),
    `VALOR_CUSTO_TOTAL_FALTANDO_DESPESAS` = VALUES(`VALOR_CUSTO_TOTAL_FALTANDO_DESPESAS`),
    `NOME_ATIVIDADE` = VALUES(`NOME_ATIVIDADE`),
    `B_DT_FIM_ATIVIDADE` = VALUES(`B_DT_FIM_ATIVIDADE`),
    `B_DT_INICIO_ATIVIDADE` = VALUES(`B_DT_INICIO_ATIVIDADE`),
    `DT_FIM_ATIVIDADE` = VALUES(`DT_FIM_ATIVIDADE`),
    `DT_INICIO_ATIVIDADE` = VALUES(`DT_INICIO_ATIVIDADE`),
    `DURACAO_PREVISTA_HORAS` = VALUES(`DURACAO_PREVISTA_HORAS`),
    `TRABALHO_TOTAL_APONTADO_ATIVIDADE` = VALUES(`TRABALHO_TOTAL_APONTADO_ATIVIDADE`),
    `TRABALHO_TOTAL_FALTANDO_ATIVIDADE` = VALUES(`TRABALHO_TOTAL_FALTANDO_ATIVIDADE`),
    `TRABALHO_TOTAL_PREVISTO_ATIVIDADE` = VALUES(`TRABALHO_TOTAL_PREVISTO_ATIVIDADE`),
    `PROJREC_ID` = VALUES(`PROJREC_ID`),
    `USU_ID` = VALUES(`USU_ID`),
    `TRABALHO_RECURSO_APONTADO_ATIVIDADE` = VALUES(`TRABALHO_RECURSO_APONTADO_ATIVIDADE`),
    `TRABALHO_RECURSO_FALTANDO_ATIVIDADE` = VALUES(`TRABALHO_RECURSO_FALTANDO_ATIVIDADE`),
    `TRABALHO_RECURSO_PREVISTO_ATIVIDADE` = VALUES(`TRABALHO_RECURSO_PREVISTO_ATIVIDADE`),
    `PERC_ALOCACAO` = VALUES(`PERC_ALOCACAO`),
    `NOME_RECURSO` = VALUES(`NOME_RECURSO`),
    `DESCRICAO` = VALUES(`DESCRICAO`),
    `TX_ID_RECURSO` = VALUES(`TX_ID_RECURSO`);
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
    
    if column_name in ["DT_FIM_PROJETO", "DT_INICIO_PROJETO", "DT_CALCULO", "B_DT_FIM_ATIVIDADE", "B_DT_INICIO_ATIVIDADE", "DT_FIM_ATIVIDADE", "DT_INICIO_ATIVIDADE"]:
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
