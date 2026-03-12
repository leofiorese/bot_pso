import logging
import pandas as pd
from utils import arquivar_csv
from actions.upsert_data.pg_upsert_utils import bulk_upsert

TABLE_COLUMNS = [
    "FUNC_ID", "NOME", "DESCRICAO", "FUNC_ID_PAI", "TAXA_ID_FAT",
    "TAXA_ID_CUS", "ORDEM", "CALC_ORDEM", "CALC_NIVEL",
    "CALC_HIERARQUIA", "INCLUIDO_EM", "ALTERADO_EM"
]

TABLE_NAME = "AGRUPAMENTO"
PK_COLUMNS = ["FUNC_ID"]
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
