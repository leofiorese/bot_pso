import logging
import pandas as pd
from utils import arquivar_csv
from actions.upsert_data.pg_upsert_utils import bulk_upsert

TABLE_COLUMNS = [
    "USU_ID", "NOME", "SIGLA", "LOGIN", "EMAIL", "ATIVO",
    "USU_ID_SUPERIOR", "EMP_ID", "FORN_ID", "CR_ID", "CPF", "RG",
    "TELEFONE", "ENDERECO", "SEXO", "DADOS_PAGAMENTO", "DESCRICAO",
    "OBS", "TAXA_ID", "TAXA_ID_HORARIA", "TAXA_ID_HORARIA_2",
    "TAXA_ID_CUS_MENSAL", "TAXA_ID_CUS", "TAXA_ID_CUS_2",
    "DT_ADMISSAO", "DT_DESLIGAMENTO", "DT_NASCIMENTO", "PERFIS",
    "SALDO_DESPESAS", "DT_ULT_MOV_DESP", "IND_BANCO_HORAS",
    "IND_APONTAMENTO_HORAS", "DT_BANCO_HORAS", "CAP_PROD_DIA",
    "INCLUIDO_EM", "ALTERADO_EM", "MYFINANCE_PEOPLE_ID",
    "CALENDAR_KEY", "TRELLO_USER_ID", "UDF1", "UDF2", "UDF3", "UDF4",
    "UDF5", "UDF6", "UDF7", "UDF8", "UDF9", "UDF10",
    "TIPO_AUTENTICACAO", "EXTERNAL_ID", "AGILE_USER_ID",
    "UDF11", "UDF12", "UDF13", "UDF14", "UDF15", "JIRA_ACCOUNT_ID",
    "LIMITE_HORAS_DIA", "UDF16", "UDF17", "UDF18", "UDF19", "UDF20"
]

TABLE_NAME = "INFO_COLABS"
PK_COLUMNS = ["USU_ID"]
DATE_COLUMNS = [
    "DT_ADMISSAO", "DT_DESLIGAMENTO", "DT_NASCIMENTO",
    "DT_ULT_MOV_DESP", "DT_BANCO_HORAS", "INCLUIDO_EM", "ALTERADO_EM",
]
BOOLEAN_COLUMNS = ["ATIVO", "IND_BANCO_HORAS", "IND_APONTAMENTO_HORAS"]


def upsert_data(df: pd.DataFrame, table_name: str, csv_file_path: str):
    bulk_upsert(
        df=df, table_name=TABLE_NAME,
        all_columns=TABLE_COLUMNS, pk_columns=PK_COLUMNS,
        date_columns=DATE_COLUMNS, boolean_columns=BOOLEAN_COLUMNS,
        csv_file_path=csv_file_path,
        archive_func=arquivar_csv, archive_name=table_name,
    )
