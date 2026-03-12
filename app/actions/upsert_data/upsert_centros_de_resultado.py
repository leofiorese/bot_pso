import logging
import pandas as pd
from utils import arquivar_csv
from actions.upsert_data.pg_upsert_utils import bulk_upsert

TABLE_COLUMNS = [
    "CR_ID", "NOME", "DESCRICAO", "EMP_ID", "CR_ID_PAI",
    "USU_ID_GER", "USU_ID_ATD_SOL", "USU_ID_PAG_SOL",
    "USU_ID_APROV_DESPESAS", "CALC_ORDEM", "CALC_NIVEL",
    "CALC_HIERARQUIA", "IND_ATIVO", "IND_ADIANTAMENTOS",
    "INCLUIDO_EM", "ALTERADO_EM", "UDF1", "UDF2", "UDF3", "UDF4",
    "UDF5", "UDF6", "UDF7", "UDF8", "UDF9", "UDF10"
]

TABLE_NAME = "CENTROS_DE_RESULTADO"
PK_COLUMNS = ["CR_ID"]
DATE_COLUMNS = ["INCLUIDO_EM", "ALTERADO_EM"]
BOOLEAN_COLUMNS = ["IND_ATIVO", "IND_ADIANTAMENTOS"]


def upsert_data(df: pd.DataFrame, table_name: str, csv_file_path: str):
    bulk_upsert(
        df=df, table_name=TABLE_NAME,
        all_columns=TABLE_COLUMNS, pk_columns=PK_COLUMNS,
        date_columns=DATE_COLUMNS, boolean_columns=BOOLEAN_COLUMNS,
        csv_file_path=csv_file_path,
        archive_func=arquivar_csv, archive_name=table_name,
    )
