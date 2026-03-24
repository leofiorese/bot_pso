import logging
import pandas as pd
from utils import arquivar_csv
from actions.upsert_data.pg_upsert_utils import bulk_upsert

TABLE_COLUMNS = [
    "PROJ_ID", "DESPT_ID", "REEMBOLSAVEL", "COBRAVEL",
    "RGR_VALOR_MAXIMO", "VALOR_PREVISTO", "INCLUIDO_EM", "ALTERADO_EM",
    "LIMITE_APONTAMENTO", "LIMITE_MAXIMO", "APON_BLOQUEADO",
    "TAXA_ID_PGTO", "TAXA_ID_FAT"
]

TABLE_NAME = "DESPESA_ORCADA"
PK_COLUMNS = ["DESPT_ID"]
DATE_COLUMNS = ["INCLUIDO_EM", "ALTERADO_EM"]
BOOLEAN_COLUMNS = ["REEMBOLSAVEL", "COBRAVEL", "APON_BLOQUEADO"]


def upsert_data(df: pd.DataFrame, table_name: str, csv_file_path: str):
    bulk_upsert(
        df=df, table_name=TABLE_NAME,
        all_columns=TABLE_COLUMNS, pk_columns=PK_COLUMNS,
        date_columns=DATE_COLUMNS, boolean_columns=BOOLEAN_COLUMNS,
        csv_file_path=csv_file_path,
        archive_func=arquivar_csv, archive_name="DESPESA ORÇADA",
    )
