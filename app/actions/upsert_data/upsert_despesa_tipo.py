import logging
import pandas as pd
from utils import arquivar_csv
from actions.upsert_data.pg_upsert_utils import bulk_upsert

TABLE_COLUMNS = [
    "DESPT_ID", "CODIGO", "NOME", "DESCRICAO", "TIPO",
    "RGR_VALOR_MAXIMO", "IND_ATIVO", "INCLUIDO_EM", "ALTERADO_EM",
    "UDF1", "UDF2", "UDF3", "UDF4", "UDF5", "UDF6", "UDF7", "UDF8",
    "UDF9", "UDF10", "TP_APONTAMENTO", "TAXA_ID_PGTO", "TAXA_ID_FAT"
]

TABLE_NAME = "DESPESA_TIPO"
PK_COLUMNS = ["DESPT_ID"]
DATE_COLUMNS = ["INCLUIDO_EM", "ALTERADO_EM"]
BOOLEAN_COLUMNS = ["IND_ATIVO"]


def upsert_data(df: pd.DataFrame, table_name: str, csv_file_path: str):
    bulk_upsert(
        df=df, table_name=TABLE_NAME,
        all_columns=TABLE_COLUMNS, pk_columns=PK_COLUMNS,
        date_columns=DATE_COLUMNS, boolean_columns=BOOLEAN_COLUMNS,
        csv_file_path=csv_file_path,
        archive_func=arquivar_csv, archive_name=table_name,
    )
