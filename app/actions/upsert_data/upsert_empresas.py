import logging
import pandas as pd
from utils import arquivar_csv
from actions.upsert_data.pg_upsert_utils import bulk_upsert

TABLE_COLUMNS = [
    "PJ_ID", "CODIGO", "NOME", "RAZAO_SOCIAL", "CNPJ", "IESTADUAL",
    "IMUNICIPAL", "ENDERECO", "CEP", "MUNICIPIO", "ESTADO",
    "INSTRUCOES_NF", "PJ_ID_PAI", "USU_ID_GER_COM", "USU_ID_APRV_DESP",
    "ORDEM", "ALIQ_COFINS", "ALIQ_PIS", "ALIQ_ISS", "ALIQ_IRRF",
    "ALIQ_CSLL", "IND_CLIENTE", "IND_FORNECEDOR", "IND_EMPRESA",
    "CALC_ORDEM", "CALC_NIVEL", "CALC_HIERARQUIA", "INCLUIDO_EM",
    "ALTERADO_EM", "CAL_ID", "EMAIL_COBRANCA", "EMAIL_COBRANCA_CC",
    "EMAIL_COBRANCA_CCO", "EMAIL_COBRANCA_REP", "EMAIL_COBRANCA_SUB",
    "IND_EMISSAO_RPS", "RPS_NUM_INICIAL", "SERIE_RPS", "AHE_FAT",
    "AHE_PAG", "HE_ID_FAT", "HE_ID_PAG", "MYFINANCE_ENTITY_ID",
    "MYFINANCE_PEOPLE_ID", "UDF1", "UDF2", "UDF3", "UDF4", "UDF5",
    "UDF6", "UDF7", "UDF8", "UDF9", "UDF10", "EMAIL_CONTRATOS",
    "EMAIL_NOME_CONTA", "IND_ATIVO", "OMIE_TOKEN", "OMIE_KEY",
    "OMIE_FAT_LISTA_ETAPAS", "UDF11", "UDF12", "UDF13", "UDF14",
    "UDF15", "OMIE_CONTA_CORRENTE",
    "END_TP_LOGRADOURO", "END_LOGRADOURO", "END_NUMERO",
    "END_COMPLEMENTO", "END_BAIRRO", "UF", "COD_MUNICIPIO"
]

TABLE_NAME = "EMPRESAS"
PK_COLUMNS = ["PJ_ID"]
DATE_COLUMNS = ["INCLUIDO_EM", "ALTERADO_EM"]
BOOLEAN_COLUMNS = []


def upsert_data(df: pd.DataFrame, table_name: str, csv_file_path: str):
    bulk_upsert(
        df=df, table_name=TABLE_NAME,
        all_columns=TABLE_COLUMNS, pk_columns=PK_COLUMNS,
        date_columns=DATE_COLUMNS, boolean_columns=BOOLEAN_COLUMNS,
        csv_file_path=csv_file_path,
        archive_func=arquivar_csv, archive_name=table_name,
    )
