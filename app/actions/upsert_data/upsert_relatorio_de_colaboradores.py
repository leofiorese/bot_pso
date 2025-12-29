import logging
import pandas as pd
from db.db import get_conn
from datetime import datetime
import os
from utils import arquivar_csv

TABLE_COLUMNS = [
    "Login",
    "Nome",
    "Sigla",
    "Email",
    "CPF",
    "RG",
    "Telefone",
    "Endereco",
    "Sexo",
    "DadosPagamento",
    "Descricao",
    "Obs",
    "DataAdmissao",
    "DataNascimento",
    "NomeCentroResultado",
    "NomePessoaJuridica",
    "ValorTaxaHistorico",
    "IncluidoEm"
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `RELATORIO_DE_COLABORADORES` (
    `Login` VARCHAR(255),
    `Nome` VARCHAR(255),
    `Sigla` VARCHAR(100),
    `Email` VARCHAR(255),
    `CPF` VARCHAR(50),
    `RG` VARCHAR(50),
    `Telefone` VARCHAR(50),
    `Endereco` TEXT,
    `Sexo` VARCHAR(50),
    `DadosPagamento` TEXT,
    `Descricao` TEXT,
    `Obs` TEXT,
    `DataAdmissao` DATE,
    `DataNascimento` DATE,
    `NomeCentroResultado` VARCHAR(255),
    `NomePessoaJuridica` VARCHAR(255),
    `ValorTaxaHistorico` DECIMAL(10,2),
    `IncluidoEm` DATE,

    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`Login`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO RELATORIO_DE_COLABORADORES (
    Login, Nome, Sigla, Email, CPF, RG, Telefone, Endereco, Sexo, DadosPagamento,
    Descricao, Obs, DataAdmissao, DataNascimento, NomeCentroResultado, NomePessoaJuridica,
    ValorTaxaHistorico, IncluidoEm
) VALUES (
    %(Login)s, %(Nome)s, %(Sigla)s, %(Email)s, %(CPF)s, %(RG)s, %(Telefone)s, %(Endereco)s, %(Sexo)s, %(DadosPagamento)s,
    %(Descricao)s, %(Obs)s, %(DataAdmissao)s, %(DataNascimento)s, %(NomeCentroResultado)s, %(NomePessoaJuridica)s,
    %(ValorTaxaHistorico)s, %(IncluidoEm)s
)
ON DUPLICATE KEY UPDATE
    Nome = VALUES(Nome),
    Sigla = VALUES(Sigla),
    Email = VALUES(Email),
    CPF = VALUES(CPF),
    RG = VALUES(RG),
    Telefone = VALUES(Telefone),
    Endereco = VALUES(Endereco),
    Sexo = VALUES(Sexo),
    DadosPagamento = VALUES(DadosPagamento),
    Descricao = VALUES(Descricao),
    Obs = VALUES(Obs),
    DataAdmissao = VALUES(DataAdmissao),
    DataNascimento = VALUES(DataNascimento),
    NomeCentroResultado = VALUES(NomeCentroResultado),
    NomePessoaJuridica = VALUES(NomePessoaJuridica),
    ValorTaxaHistorico = VALUES(ValorTaxaHistorico),
    IncluidoEm = VALUES(IncluidoEm);
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
    if column_name in ["DataAdmissao", "DataNascimento", "IncluidoEm"]:
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
        arquivar_csv(csv_file_path, "RELATORIO DE COLABORADORES")
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