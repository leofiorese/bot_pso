import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os

TABLE_COLUMNS = [
    "USU_ID",
    "NOME",
    "SIGLA",
    "LOGIN",
    "EMAIL",
    "ATIVO",
    "USU_ID_SUPERIOR",
    "EMP_ID",
    "FORN_ID",
    "CR_ID",
    "CPF",
    "RG",
    "TELEFONE",
    "ENDERECO",
    "SEXO",
    "DADOS_PAGAMENTO",
    "DESCRICAO",
    "OBS",
    "TAXA_ID",
    "TAXA_ID_HORARIA",
    "TAXA_ID_HORARIA_2",
    "TAXA_ID_CUS_MENSAL",
    "TAXA_ID_CUS",
    "TAXA_ID_CUS_2",
    "DT_ADMISSAO",
    "DT_DESLIGAMENTO",
    "DT_NASCIMENTO",
    "PERFIS",
    "SALDO_DESPESAS",
    "DT_ULT_MOV_DESP",
    "IND_BANCO_HORAS",
    "IND_APONTAMENTO_HORAS",
    "DT_BANCO_HORAS",
    "CAP_PROD_DIA",
    "INCLUIDO_EM",
    "ALTERADO_EM",
    "MYFINANCE_PEOPLE_ID",
    "CALENDAR_KEY",
    "TRELLO_USER_ID",
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
    "TIPO_AUTENTICACAO",
    "EXTERNAL_ID",
    "AGILE_USER_ID",
    "UDF11",
    "UDF12",
    "UDF13",
    "UDF14",
    "UDF15",
    "JIRA_ACCOUNT_ID",
    "LIMITE_HORAS_DIA",
    "UDF16",
    "UDF17",
    "UDF18",
    "UDF19",
    "UDF20"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `INFO_COLABS` (
    `USU_ID` INT,
    `NOME` VARCHAR(255),
    `SIGLA` VARCHAR(50),
    `LOGIN` VARCHAR(255),
    `EMAIL` VARCHAR(255),
    `ATIVO` BOOLEAN,
    `USU_ID_SUPERIOR` INT,
    `EMP_ID` INT,
    `FORN_ID` INT,
    `CR_ID` INT,
    `CPF` VARCHAR(20),
    `RG` VARCHAR(50),
    `TELEFONE` VARCHAR(50),
    `ENDERECO` VARCHAR(255),
    `SEXO` VARCHAR(1),
    `DADOS_PAGAMENTO` TEXT,
    `DESCRICAO` TEXT,
    `OBS` TEXT,
    `TAXA_ID` INT,
    `TAXA_ID_HORARIA` INT,
    `TAXA_ID_HORARIA_2` INT,
    `TAXA_ID_CUS_MENSAL` INT,
    `TAXA_ID_CUS` INT,
    `TAXA_ID_CUS_2` INT,
    `DT_ADMISSAO` DATE,
    `DT_DESLIGAMENTO` DATE,
    `DT_NASCIMENTO` DATE,
    `PERFIS` TEXT,
    `SALDO_DESPESAS` DECIMAL(18,2),
    `DT_ULT_MOV_DESP` DATE,
    `IND_BANCO_HORAS` BOOLEAN,
    `IND_APONTAMENTO_HORAS` BOOLEAN,
    `DT_BANCO_HORAS` DATE,
    `CAP_PROD_DIA` DECIMAL(10,2),
    `INCLUIDO_EM` DATE,
    `ALTERADO_EM` DATE,
    `MYFINANCE_PEOPLE_ID` INT,
    `CALENDAR_KEY` VARCHAR(255),
    `TRELLO_USER_ID` VARCHAR(255),
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
    `TIPO_AUTENTICACAO` VARCHAR(50),
    `EXTERNAL_ID` VARCHAR(255),
    `AGILE_USER_ID` VARCHAR(255),
    `UDF11` VARCHAR(255),
    `UDF12` VARCHAR(255),
    `UDF13` VARCHAR(255),
    `UDF14` VARCHAR(255),
    `UDF15` VARCHAR(255),
    `JIRA_ACCOUNT_ID` VARCHAR(255),
    `LIMITE_HORAS_DIA` DECIMAL(10,2),
    `UDF16` VARCHAR(255),
    `UDF17` VARCHAR(255),
    `UDF18` VARCHAR(255),
    `UDF19` VARCHAR(255),
    `UDF20` VARCHAR(255),

    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`USU_ID`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO `INFO_COLABS` (
    USU_ID, NOME, SIGLA, LOGIN, EMAIL, ATIVO, USU_ID_SUPERIOR, EMP_ID, FORN_ID, CR_ID, CPF, RG, TELEFONE, ENDERECO, SEXO,
    DADOS_PAGAMENTO,  DESCRICAO, OBS, TAXA_ID, TAXA_ID_HORARIA, TAXA_ID_HORARIA_2, TAXA_ID_CUS_MENSAL, TAXA_ID_CUS, TAXA_ID_CUS_2,
    DT_ADMISSAO, DT_DESLIGAMENTO, DT_NASCIMENTO, PERFIS, SALDO_DESPESAS, DT_ULT_MOV_DESP, IND_BANCO_HORAS, IND_APONTAMENTO_HORAS,
    DT_BANCO_HORAS, CAP_PROD_DIA, INCLUIDO_EM, ALTERADO_EM, MYFINANCE_PEOPLE_ID, CALENDAR_KEY, TRELLO_USER_ID,
    UDF1, UDF2, UDF3, UDF4, UDF5, UDF6, UDF7, UDF8, UDF9, UDF10,
    TIPO_AUTENTICACAO, EXTERNAL_ID, AGILE_USER_ID, UDF11, UDF12, UDF13, UDF14, UDF15, JIRA_ACCOUNT_ID, LIMITE_HORAS_DIA,
    UDF16, UDF17, UDF18, UDF19, UDF20
) VALUES (
    %(USU_ID)s, %(NOME)s, %(SIGLA)s, %(LOGIN)s, %(EMAIL)s, %(ATIVO)s, %(USU_ID_SUPERIOR)s, %(EMP_ID)s, %(FORN_ID)s, %(CR_ID)s, %(CPF)s, %(RG)s, %(TELEFONE)s, %(ENDERECO)s, %(SEXO)s,
    %(DADOS_PAGAMENTO)s, %(DESCRICAO)s, %(OBS)s, %(TAXA_ID)s, %(TAXA_ID_HORARIA)s, %(TAXA_ID_HORARIA_2)s, %(TAXA_ID_CUS_MENSAL)s, %(TAXA_ID_CUS)s, %(TAXA_ID_CUS_2)s,
    %(DT_ADMISSAO)s, %(DT_DESLIGAMENTO)s, %(DT_NASCIMENTO)s, %(PERFIS)s, %(SALDO_DESPESAS)s, %(DT_ULT_MOV_DESP)s, %(IND_BANCO_HORAS)s, %(IND_APONTAMENTO_HORAS)s,
    %(DT_BANCO_HORAS)s, %(CAP_PROD_DIA)s, %(INCLUIDO_EM)s, %(ALTERADO_EM)s, %(MYFINANCE_PEOPLE_ID)s, %(CALENDAR_KEY)s, %(TRELLO_USER_ID)s,
    %(UDF1)s, %(UDF2)s, %(UDF3)s, %(UDF4)s, %(UDF5)s, %(UDF6)s, %(UDF7)s, %(UDF8)s, %(UDF9)s, %(UDF10)s,
    %(TIPO_AUTENTICACAO)s, %(EXTERNAL_ID)s, %(AGILE_USER_ID)s, %(UDF11)s, %(UDF12)s, %(UDF13)s, %(UDF14)s, %(UDF15)s, %(JIRA_ACCOUNT_ID)s, %(LIMITE_HORAS_DIA)s,
    %(UDF16)s, %(UDF17)s, %(UDF18)s, %(UDF19)s, %(UDF20)s
)
ON DUPLICATE KEY UPDATE
    NOME = VALUES(NOME),
    SIGLA = VALUES(SIGLA),
    LOGIN = VALUES(LOGIN),
    EMAIL = VALUES(EMAIL),
    ATIVO = VALUES(ATIVO),
    USU_ID_SUPERIOR = VALUES(USU_ID_SUPERIOR),
    EMP_ID = VALUES(EMP_ID),
    FORN_ID = VALUES(FORN_ID),
    CR_ID = VALUES(CR_ID),
    CPF = VALUES(CPF),
    RG = VALUES(RG),
    TELEFONE = VALUES(TELEFONE),
    ENDERECO = VALUES(ENDERECO),
    SEXO = VALUES(SEXO),
    DADOS_PAGAMENTO = VALUES(DADOS_PAGAMENTO),
    DESCRICAO = VALUES(DESCRICAO),
    OBS = VALUES(OBS),
    TAXA_ID = VALUES(TAXA_ID),
    TAXA_ID_HORARIA = VALUES(TAXA_ID_HORARIA),
    TAXA_ID_HORARIA_2 = VALUES(TAXA_ID_HORARIA_2),
    TAXA_ID_CUS_MENSAL = VALUES(TAXA_ID_CUS_MENSAL),
    TAXA_ID_CUS = VALUES(TAXA_ID_CUS),
    TAXA_ID_CUS_2 = VALUES(TAXA_ID_CUS_2),
    DT_ADMISSAO = VALUES(DT_ADMISSAO),
    DT_DESLIGAMENTO = VALUES(DT_DESLIGAMENTO),
    DT_NASCIMENTO = VALUES(DT_NASCIMENTO),
    PERFIS = VALUES(PERFIS),
    SALDO_DESPESAS = VALUES(SALDO_DESPESAS),
    DT_ULT_MOV_DESP = VALUES(DT_ULT_MOV_DESP),
    IND_BANCO_HORAS = VALUES(IND_BANCO_HORAS),
    IND_APONTAMENTO_HORAS = VALUES(IND_APONTAMENTO_HORAS),
    DT_BANCO_HORAS = VALUES(DT_BANCO_HORAS),
    CAP_PROD_DIA = VALUES(CAP_PROD_DIA),
    INCLUIDO_EM = VALUES(INCLUIDO_EM),
    ALTERADO_EM = VALUES(ALTERADO_EM),
    MYFINANCE_PEOPLE_ID = VALUES(MYFINANCE_PEOPLE_ID),
    CALENDAR_KEY = VALUES(CALENDAR_KEY),
    TRELLO_USER_ID = VALUES(TRELLO_USER_ID),
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
    TIPO_AUTENTICACAO = VALUES(TIPO_AUTENTICACAO),
    EXTERNAL_ID = VALUES(EXTERNAL_ID),
    AGILE_USER_ID = VALUES(AGILE_USER_ID),
    UDF11 = VALUES(UDF11),
    UDF12 = VALUES(UDF12),
    UDF13 = VALUES(UDF13),
    UDF14 = VALUES(UDF14),
    UDF15 = VALUES(UDF15),
    JIRA_ACCOUNT_ID = VALUES(JIRA_ACCOUNT_ID),
    LIMITE_HORAS_DIA = VALUES(LIMITE_HORAS_DIA),
    UDF16 = VALUES(UDF16),
    UDF17 = VALUES(UDF17),
    UDF18 = VALUES(UDF18),
    UDF19 = VALUES(UDF19),
    UDF20 = VALUES(UDF20);
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
        "DT_ADMISSAO","DT_DESLIGAMENTO","DT_NASCIMENTO","DT_ULT_MOV_DESP",
        "DT_BANCO_HORAS","INCLUIDO_EM","ALTERADO_EM"
    ]:
        return convert_date(value)
    if column_name in ["ATIVO", "IND_BANCO_HORAS", "IND_APONTAMENTO_HORAS"]:
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
