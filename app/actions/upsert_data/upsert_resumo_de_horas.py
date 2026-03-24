import logging
import pandas as pd
from utils import arquivar_csv
from actions.upsert_data.pg_upsert_utils import bulk_upsert

TABLE_COLUMNS = [
    "RESHR_ID", "USU_ID", "DT_INICIO", "STATUS", "DIA1", "DIA2",
    "DIA3", "DIA4", "DIA5", "DIA6", "DIA7", "ATIV_ADDED",
    "INCLUIDO_EM", "ALTERADO_EM"
]

TABLE_NAME = "RESUMO_DE_HORAS"
PK_COLUMNS = ["RESHR_ID"]
DATE_COLUMNS = ["DT_INICIO", "INCLUIDO_EM", "ALTERADO_EM"]
BOOLEAN_COLUMNS = []


def upsert_data(df: pd.DataFrame, table_name: str, csv_file_path: str):
    bulk_upsert(
        df=df, table_name=TABLE_NAME,
        all_columns=TABLE_COLUMNS, pk_columns=PK_COLUMNS,
        date_columns=DATE_COLUMNS, boolean_columns=BOOLEAN_COLUMNS,
        csv_file_path=csv_file_path,
        archive_func=arquivar_csv, archive_name="RESUMO DE HORAS",
    )
