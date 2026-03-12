import logging
import pandas as pd
from utils import arquivar_csv
from actions.upsert_data.pg_upsert_utils import bulk_upsert

TABLE_COLUMNS = [
    "ATRIB_ID", "ATIV_ID", "PROJREC_ID", "USU_ID", "FUNC_ID",
    "INDICE", "TRABALHO_PREVISTO", "TRABALHO_REALIZADO",
    "TRABALHO_APONTADO", "TRABALHO_FALTANDO", "B_TRABALHO_PREVISTO",
    "PERC_ALOCACAO", "ESTADO", "INCLUIDO_EM", "ALTERADO_EM",
    "IND_ENCERRADA"
]

TABLE_NAME = "ATRIBUICOES"
PK_COLUMNS = ["ATRIB_ID"]
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
