import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "DESP_ID",
    "PROJ_ID",
    "CR_ID",
    "DESPT_ID",
    "RESDESP_ID",
    "TIPO",
    "TIPO_RESUMO",
    "USU_ID",
    "USU_ID_APROVADOR",
    "USU_ID_APROVACAO_1",
    "USU_ID_APROVACAO_2",
    "USU_ID_REJEICAO_1",
    "USU_ID_REJEICAO_2",
    "IND_APROVADO",
    "DT_DATA",
    "DESCRICAO",
    "VALOR",
    "VALOR_FATURADO",
    "VALOR_RECONHECIDO",
    "REEMBOLSAVEL",
    "COBRAVEL",
    "STATUS",
    "IND_DCTO_ANEXO",
    "FAT_ID",
    "PAG_ID",
    "INCLUIDO_EM",
    "ALTERADO_EM",
    "DT_SUBMISSAO",
    "DT_APRREG_1",
    "DT_APRREG_2",
    "DT_APROVACAO",
    "AUX1",
    "DESP_UUID",
    "VALOR_ORCADO",
    "VLR_APTO",
    "VLR_APTO_FATURADO",
    "VLR_APTO_RECONHECIDO",
    "VLR_APTO_ORCADO"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `DESPESAS` (
    `DESP_ID` INT,
    `PROJ_ID` INT,
    `CR_ID` INT,
    `DESPT_ID` INT,
    `RESDESP_ID` INT,
    `TIPO` INT,
    `TIPO_RESUMO` INT,
    `USU_ID` INT,
    `USU_ID_APROVADOR` INT,
    `USU_ID_APROVACAO_1` INT,
    `USU_ID_APROVACAO_2` INT,
    `USU_ID_REJEICAO_1` INT,
    `USU_ID_REJEICAO_2` INT,
    `IND_APROVADO` VARCHAR(1),
    `DT_DATA` DATE,
    `DESCRICAO` TEXT,
    `VALOR` DECIMAL(18,2),
    `VALOR_FATURADO` DECIMAL(18,2),
    `VALOR_RECONHECIDO` DECIMAL(18,2),
    `REEMBOLSAVEL` VARCHAR(1),
    `COBRAVEL` VARCHAR(1),
    `STATUS` VARCHAR(100),
    `IND_DCTO_ANEXO` VARCHAR(1),
    `FAT_ID` INT,
    `PAG_ID` INT,
    `INCLUIDO_EM` DATE,
    `ALTERADO_EM` DATE,
    `DT_SUBMISSAO` DATE,
    `DT_APRREG_1` DATE,
    `DT_APRREG_2` DATE,
    `DT_APROVACAO` DATE,
    `AUX1` DECIMAL(18,2),
    `DESP_UUID` VARCHAR(64),
    `VALOR_ORCADO` DECIMAL(18,2),
    `VLR_APTO` DECIMAL(18,2),
    `VLR_APTO_FATURADO` DECIMAL(18,2),
    `VLR_APTO_RECONHECIDO` DECIMAL(18,2),
    `VLR_APTO_ORCADO` DECIMAL(18,2),

    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`DESP_ID`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO DESPESAS (
    DESP_ID, PROJ_ID, CR_ID, DESPT_ID, RESDESP_ID, TIPO, TIPO_RESUMO, USU_ID,
    USU_ID_APROVADOR, USU_ID_APROVACAO_1, USU_ID_APROVACAO_2, USU_ID_REJEICAO_1, USU_ID_REJEICAO_2,
    IND_APROVADO, DT_DATA, DESCRICAO, VALOR, VALOR_FATURADO, VALOR_RECONHECIDO, REEMBOLSAVEL, COBRAVEL,
    STATUS, IND_DCTO_ANEXO, FAT_ID, PAG_ID, INCLUIDO_EM, ALTERADO_EM, DT_SUBMISSAO, DT_APRREG_1, DT_APRREG_2,
    DT_APROVACAO, AUX1, DESP_UUID, VALOR_ORCADO, VLR_APTO, VLR_APTO_FATURADO, VLR_APTO_RECONHECIDO, VLR_APTO_ORCADO
) VALUES (
    %(DESP_ID)s, %(PROJ_ID)s, %(CR_ID)s, %(DESPT_ID)s, %(RESDESP_ID)s, %(TIPO)s, %(TIPO_RESUMO)s, %(USU_ID)s,
    %(USU_ID_APROVADOR)s, %(USU_ID_APROVACAO_1)s, %(USU_ID_APROVACAO_2)s, %(USU_ID_REJEICAO_1)s, %(USU_ID_REJEICAO_2)s,
    %(IND_APROVADO)s, %(DT_DATA)s, %(DESCRICAO)s, %(VALOR)s, %(VALOR_FATURADO)s, %(VALOR_RECONHECIDO)s, %(REEMBOLSAVEL)s, %(COBRAVEL)s,
    %(STATUS)s, %(IND_DCTO_ANEXO)s, %(FAT_ID)s, %(PAG_ID)s, %(INCLUIDO_EM)s, %(ALTERADO_EM)s, %(DT_SUBMISSAO)s, %(DT_APRREG_1)s, %(DT_APRREG_2)s,
    %(DT_APROVACAO)s, %(AUX1)s, %(DESP_UUID)s, %(VALOR_ORCADO)s, %(VLR_APTO)s, %(VLR_APTO_FATURADO)s, %(VLR_APTO_RECONHECIDO)s, %(VLR_APTO_ORCADO)s
)
ON DUPLICATE KEY UPDATE
    PROJ_ID = VALUES(PROJ_ID),
    CR_ID = VALUES(CR_ID),
    DESPT_ID = VALUES(DESPT_ID),
    RESDESP_ID = VALUES(RESDESP_ID),
    TIPO = VALUES(TIPO),
    TIPO_RESUMO = VALUES(TIPO_RESUMO),
    USU_ID = VALUES(USU_ID),
    USU_ID_APROVADOR = VALUES(USU_ID_APROVADOR),
    USU_ID_APROVACAO_1 = VALUES(USU_ID_APROVACAO_1),
    USU_ID_APROVACAO_2 = VALUES(USU_ID_APROVACAO_2),
    USU_ID_REJEICAO_1 = VALUES(USU_ID_REJEICAO_1),
    USU_ID_REJEICAO_2 = VALUES(USU_ID_REJEICAO_2),
    IND_APROVADO = VALUES(IND_APROVADO),
    DT_DATA = VALUES(DT_DATA),
    DESCRICAO = VALUES(DESCRICAO),
    VALOR = VALUES(VALOR),
    VALOR_FATURADO = VALUES(VALOR_FATURADO),
    VALOR_RECONHECIDO = VALUES(VALOR_RECONHECIDO),
    REEMBOLSAVEL = VALUES(REEMBOLSAVEL),
    COBRAVEL = VALUES(COBRAVEL),
    STATUS = VALUES(STATUS),
    IND_DCTO_ANEXO = VALUES(IND_DCTO_ANEXO),
    FAT_ID = VALUES(FAT_ID),
    PAG_ID = VALUES(PAG_ID),
    INCLUIDO_EM = VALUES(INCLUIDO_EM),
    ALTERADO_EM = VALUES(ALTERADO_EM),
    DT_SUBMISSAO = VALUES(DT_SUBMISSAO),
    DT_APRREG_1 = VALUES(DT_APRREG_1),
    DT_APRREG_2 = VALUES(DT_APRREG_2),
    DT_APROVACAO = VALUES(DT_APROVACAO),
    AUX1 = VALUES(AUX1),
    DESP_UUID = VALUES(DESP_UUID),
    VALOR_ORCADO = VALUES(VALOR_ORCADO),
    VLR_APTO = VALUES(VLR_APTO),
    VLR_APTO_FATURADO = VALUES(VLR_APTO_FATURADO),
    VLR_APTO_RECONHECIDO = VALUES(VLR_APTO_RECONHECIDO),
    VLR_APTO_ORCADO = VALUES(VLR_APTO_ORCADO);
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
        "DT_DATA","INCLUIDO_EM","ALTERADO_EM","DT_SUBMISSAO","DT_APRREG_1","DT_APRREG_2","DT_APROVACAO"
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
