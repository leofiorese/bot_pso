import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "PJ_ID",
    "CODIGO",
    "NOME",
    "RAZAO_SOCIAL",
    "CNPJ",
    "IESTADUAL",
    "IMUNICIPAL",
    "ENDERECO",
    "CEP",
    "MUNICIPIO",
    "ESTADO",
    "INSTRUCOES_NF",
    "PJ_ID_PAI",
    "USU_ID_GER_COM",
    "USU_ID_APRV_DESP",
    "ORDEM",
    "ALIQ_COFINS",
    "ALIQ_PIS",
    "ALIQ_ISS",
    "ALIQ_IRRF",
    "ALIQ_CSLL",
    "IND_CLIENTE",
    "IND_FORNECEDOR",
    "IND_EMPRESA",
    "CALC_ORDEM",
    "CALC_NIVEL",
    "CALC_HIERARQUIA",
    "INCLUIDO_EM",
    "ALTERADO_EM",
    "CAL_ID",
    "EMAIL_COBRANCA",
    "EMAIL_COBRANCA_CC",
    "EMAIL_COBRANCA_CCO",
    "EMAIL_COBRANCA_REP",
    "EMAIL_COBRANCA_SUB",
    "IND_EMISSAO_RPS",
    "RPS_NUM_INICIAL",
    "SERIE_RPS",
    "AHE_FAT",
    "AHE_PAG",
    "HE_ID_FAT",
    "HE_ID_PAG",
    "MYFINANCE_ENTITY_ID",
    "MYFINANCE_PEOPLE_ID",
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
    "EMAIL_CONTRATOS",
    "EMAIL_NOME_CONTA",
    "IND_ATIVO",
    "OMIE_TOKEN",
    "OMIE_KEY",
    "OMIE_FAT_LISTA_ETAPAS",
    "UDF11",
    "UDF12",
    "UDF13",
    "UDF14",
    "UDF15",
    "OMIE_CONTA_CORRENTE"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `EMPRESAS` (
    `PJ_ID` INT,
    `CODIGO` VARCHAR(255),
    `NOME` VARCHAR(255),
    `RAZAO_SOCIAL` VARCHAR(255),
    `CNPJ` VARCHAR(20),
    `IESTADUAL` VARCHAR(50),
    `IMUNICIPAL` VARCHAR(50),
    `ENDERECO` VARCHAR(255),
    `CEP` VARCHAR(20),
    `MUNICIPIO` VARCHAR(100),
    `ESTADO` VARCHAR(50),
    `INSTRUCOES_NF` TEXT,
    `PJ_ID_PAI` INT,
    `USU_ID_GER_COM` INT,
    `USU_ID_APRV_DESP` INT,
    `ORDEM` INT,
    `ALIQ_COFINS` DECIMAL(10,2),
    `ALIQ_PIS` DECIMAL(10,2),
    `ALIQ_ISS` DECIMAL(10,2),
    `ALIQ_IRRF` DECIMAL(10,2),
    `ALIQ_CSLL` DECIMAL(10,2),
    `IND_CLIENTE` VARCHAR(1),
    `IND_FORNECEDOR` VARCHAR(1),
    `IND_EMPRESA` VARCHAR(1),
    `CALC_ORDEM` INT,
    `CALC_NIVEL` INT,
    `CALC_HIERARQUIA` DECIMAL(18,2),
    `INCLUIDO_EM` DATE,
    `ALTERADO_EM` DATE,
    `CAL_ID` INT,
    `EMAIL_COBRANCA` VARCHAR(255),
    `EMAIL_COBRANCA_CC` VARCHAR(255),
    `EMAIL_COBRANCA_CCO` VARCHAR(255),
    `EMAIL_COBRANCA_REP` VARCHAR(255),
    `EMAIL_COBRANCA_SUB` VARCHAR(255),
    `IND_EMISSAO_RPS` VARCHAR(1),
    `RPS_NUM_INICIAL` INT,
    `SERIE_RPS` VARCHAR(50),
    `AHE_FAT` VARCHAR(1),
    `AHE_PAG` VARCHAR(1),
    `HE_ID_FAT` INT,
    `HE_ID_PAG` INT,
    `MYFINANCE_ENTITY_ID` INT,
    `MYFINANCE_PEOPLE_ID` INT,
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
    `EMAIL_CONTRATOS` VARCHAR(255),
    `EMAIL_NOME_CONTA` VARCHAR(255),
    `IND_ATIVO` VARCHAR(1),
    `OMIE_TOKEN` VARCHAR(255),
    `OMIE_KEY` VARCHAR(255),
    `OMIE_FAT_LISTA_ETAPAS` VARCHAR(255),
    `UDF11` VARCHAR(255),
    `UDF12` VARCHAR(255),
    `UDF13` VARCHAR(255),
    `UDF14` VARCHAR(255),
    `UDF15` VARCHAR(255),
    `OMIE_CONTA_CORRENTE` INT,

    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`PJ_ID`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO EMPRESAS (
    PJ_ID, CODIGO, NOME, RAZAO_SOCIAL, CNPJ, IESTADUAL, IMUNICIPAL, ENDERECO, CEP, MUNICIPIO, ESTADO,
    INSTRUCOES_NF, PJ_ID_PAI, USU_ID_GER_COM, USU_ID_APRV_DESP, ORDEM, ALIQ_COFINS, ALIQ_PIS, ALIQ_ISS, ALIQ_IRRF,
    ALIQ_CSLL, IND_CLIENTE, IND_FORNECEDOR, IND_EMPRESA, CALC_ORDEM, CALC_NIVEL, CALC_HIERARQUIA, INCLUIDO_EM,
    ALTERADO_EM, CAL_ID, EMAIL_COBRANCA, EMAIL_COBRANCA_CC, EMAIL_COBRANCA_CCO, EMAIL_COBRANCA_REP, EMAIL_COBRANCA_SUB,
    IND_EMISSAO_RPS, RPS_NUM_INICIAL, SERIE_RPS, AHE_FAT, AHE_PAG, HE_ID_FAT, HE_ID_PAG, MYFINANCE_ENTITY_ID,
    MYFINANCE_PEOPLE_ID, UDF1, UDF2, UDF3, UDF4, UDF5, UDF6, UDF7, UDF8, UDF9, UDF10, EMAIL_CONTRATOS, EMAIL_NOME_CONTA,
    IND_ATIVO, OMIE_TOKEN, OMIE_KEY, OMIE_FAT_LISTA_ETAPAS, UDF11, UDF12, UDF13, UDF14, UDF15, OMIE_CONTA_CORRENTE
) VALUES (
    %(PJ_ID)s, %(CODIGO)s, %(NOME)s, %(RAZAO_SOCIAL)s, %(CNPJ)s, %(IESTADUAL)s, %(IMUNICIPAL)s, %(ENDERECO)s, %(CEP)s, %(MUNICIPIO)s, %(ESTADO)s,
    %(INSTRUCOES_NF)s, %(PJ_ID_PAI)s, %(USU_ID_GER_COM)s, %(USU_ID_APRV_DESP)s, %(ORDEM)s, %(ALIQ_COFINS)s, %(ALIQ_PIS)s, %(ALIQ_ISS)s, %(ALIQ_IRRF)s,
    %(ALIQ_CSLL)s, %(IND_CLIENTE)s, %(IND_FORNECEDOR)s, %(IND_EMPRESA)s, %(CALC_ORDEM)s, %(CALC_NIVEL)s, %(CALC_HIERARQUIA)s, %(INCLUIDO_EM)s,
    %(ALTERADO_EM)s, %(CAL_ID)s, %(EMAIL_COBRANCA)s, %(EMAIL_COBRANCA_CC)s, %(EMAIL_COBRANCA_CCO)s, %(EMAIL_COBRANCA_REP)s, %(EMAIL_COBRANCA_SUB)s,
    %(IND_EMISSAO_RPS)s, %(RPS_NUM_INICIAL)s, %(SERIE_RPS)s, %(AHE_FAT)s, %(AHE_PAG)s, %(HE_ID_FAT)s, %(HE_ID_PAG)s, %(MYFINANCE_ENTITY_ID)s,
    %(MYFINANCE_PEOPLE_ID)s, %(UDF1)s, %(UDF2)s, %(UDF3)s, %(UDF4)s, %(UDF5)s, %(UDF6)s, %(UDF7)s, %(UDF8)s, %(UDF9)s, %(UDF10)s, %(EMAIL_CONTRATOS)s, %(EMAIL_NOME_CONTA)s,
    %(IND_ATIVO)s, %(OMIE_TOKEN)s, %(OMIE_KEY)s, %(OMIE_FAT_LISTA_ETAPAS)s, %(UDF11)s, %(UDF12)s, %(UDF13)s, %(UDF14)s, %(UDF15)s, %(OMIE_CONTA_CORRENTE)s
)
ON DUPLICATE KEY UPDATE
    CODIGO = VALUES(CODIGO),
    NOME = VALUES(NOME),
    RAZAO_SOCIAL = VALUES(RAZAO_SOCIAL),
    CNPJ = VALUES(CNPJ),
    IESTADUAL = VALUES(IESTADUAL),
    IMUNICIPAL = VALUES(IMUNICIPAL),
    ENDERECO = VALUES(ENDERECO),
    CEP = VALUES(CEP),
    MUNICIPIO = VALUES(MUNICIPIO),
    ESTADO = VALUES(ESTADO),
    INSTRUCOES_NF = VALUES(INSTRUCOES_NF),
    PJ_ID_PAI = VALUES(PJ_ID_PAI),
    USU_ID_GER_COM = VALUES(USU_ID_GER_COM),
    USU_ID_APRV_DESP = VALUES(USU_ID_APRV_DESP),
    ORDEM = VALUES(ORDEM),
    ALIQ_COFINS = VALUES(ALIQ_COFINS),
    ALIQ_PIS = VALUES(ALIQ_PIS),
    ALIQ_ISS = VALUES(ALIQ_ISS),
    ALIQ_IRRF = VALUES(ALIQ_IRRF),
    ALIQ_CSLL = VALUES(ALIQ_CSLL),
    IND_CLIENTE = VALUES(IND_CLIENTE),
    IND_FORNECEDOR = VALUES(IND_FORNECEDOR),
    IND_EMPRESA = VALUES(IND_EMPRESA),
    CALC_ORDEM = VALUES(CALC_ORDEM),
    CALC_NIVEL = VALUES(CALC_NIVEL),
    CALC_HIERARQUIA = VALUES(CALC_HIERARQUIA),
    INCLUIDO_EM = VALUES(INCLUIDO_EM),
    ALTERADO_EM = VALUES(ALTERADO_EM),
    CAL_ID = VALUES(CAL_ID),
    EMAIL_COBRANCA = VALUES(EMAIL_COBRANCA),
    EMAIL_COBRANCA_CC = VALUES(EMAIL_COBRANCA_CC),
    EMAIL_COBRANCA_CCO = VALUES(EMAIL_COBRANCA_CCO),
    EMAIL_COBRANCA_REP = VALUES(EMAIL_COBRANCA_REP),
    EMAIL_COBRANCA_SUB = VALUES(EMAIL_COBRANCA_SUB),
    IND_EMISSAO_RPS = VALUES(IND_EMISSAO_RPS),
    RPS_NUM_INICIAL = VALUES(RPS_NUM_INICIAL),
    SERIE_RPS = VALUES(SERIE_RPS),
    AHE_FAT = VALUES(AHE_FAT),
    AHE_PAG = VALUES(AHE_PAG),
    HE_ID_FAT = VALUES(HE_ID_FAT),
    HE_ID_PAG = VALUES(HE_ID_PAG),
    MYFINANCE_ENTITY_ID = VALUES(MYFINANCE_ENTITY_ID),
    MYFINANCE_PEOPLE_ID = VALUES(MYFINANCE_PEOPLE_ID),
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
    EMAIL_CONTRATOS = VALUES(EMAIL_CONTRATOS),
    EMAIL_NOME_CONTA = VALUES(EMAIL_NOME_CONTA),
    IND_ATIVO = VALUES(IND_ATIVO),
    OMIE_TOKEN = VALUES(OMIE_TOKEN),
    OMIE_KEY = VALUES(OMIE_KEY),
    OMIE_FAT_LISTA_ETAPAS = VALUES(OMIE_FAT_LISTA_ETAPAS),
    UDF11 = VALUES(UDF11),
    UDF12 = VALUES(UDF12),
    UDF13 = VALUES(UDF13),
    UDF14 = VALUES(UDF14),
    UDF15 = VALUES(UDF15),
    OMIE_CONTA_CORRENTE = VALUES(OMIE_CONTA_CORRENTE);
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
