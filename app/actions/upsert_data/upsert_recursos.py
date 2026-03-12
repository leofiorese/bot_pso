import logging
import pandas as pd
from utils import arquivar_csv
from actions.upsert_data.pg_upsert_utils import bulk_upsert

TABLE_COLUMNS = [
    "PROJREC_ID", "PROJ_ID", "NOME", "DESCRICAO", "FUNC_ID",
    "TAXA_ID_FAT", "TAXA_ID_CUS", "TAXA_ID_PAG", "INCLUIDO_EM",
    "ALTERADO_EM", "TAXA_ID_CUS_PREV", "IND_AUTOMATICO"
]

TABLE_NAME = "RECURSOS"
PK_COLUMNS = ["PROJREC_ID"]
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
