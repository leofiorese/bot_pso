import logging
import pandas as pd
from utils import arquivar_csv
from actions.upsert_data.pg_upsert_utils import bulk_upsert

TABLE_COLUMNS = [
    "USU_ID", "FUNC_ID", "PERC_APROP", "DT_INCLUSAO", "DT_ALTERACAO"
]

TABLE_NAME = "PSO_USU_FUNCOES"
PK_COLUMNS = ["USU_ID"]
DATE_COLUMNS = ["DT_INCLUSAO", "DT_ALTERACAO"]
BOOLEAN_COLUMNS = []


def upsert_data(df: pd.DataFrame, table_name: str, csv_file_path: str):
    bulk_upsert(
        df=df, table_name=TABLE_NAME,
        all_columns=TABLE_COLUMNS, pk_columns=PK_COLUMNS,
        date_columns=DATE_COLUMNS, boolean_columns=BOOLEAN_COLUMNS,
        csv_file_path=csv_file_path,
        archive_func=arquivar_csv, archive_name=table_name,
    )
