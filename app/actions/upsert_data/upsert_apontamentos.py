import logging
import pandas as pd
from utils import arquivar_csv
from actions.upsert_data.pg_upsert_utils import bulk_upsert

TABLE_COLUMNS = [
    "APON_ID", "RESHRATI_ID", "USU_ID", "RESHR_ID", "ATRIB_ID",
    "PROJREC_ID", "ATIV_ID", "PROJ_ID", "DT_INICIO", "HR_INICIO",
    "MINUTOS", "MINUTOS_FALTANDO", "MINUTOS_FATURADOS", "MINUTOS_RECONHECIDOS",
    "MINUTOS_FAT_EXT", "MINUTOS_FAT_EXT_AD", "MINUTOS_REC_EXT", "MINUTOS_REC_EXT_AD",
    "FAT_ID", "PAG_ID", "IND_RESTRITO", "COMENTARIOS", "STATUS",
    "USU_ID_APROVADOR", "USU_ID_APROVACAO_1", "USU_ID_APROVACAO_2",
    "USU_ID_REJEICAO_1", "USU_ID_REJEICAO_2", "DT_CALC_HE",
    "DT_INCLUSAO", "DT_ALTERACAO", "UDF1", "UDF2", "UDF3", "UDF4",
    "UDF5", "UDF6", "UDF7", "UDF8", "UDF9", "UDF10",
    "DET1", "AUX1", "INTR1", "INTR2", "INTR3", "INTR4", "INTR5",
    "WKLOG_JIRA_ID", "DT_SUBMISSAO", "DT_APROVACAO_1", "DT_APROVACAO_2",
]

TABLE_NAME = "APONTAMENTOS"
PK_COLUMNS = ["APON_ID"]
DATE_COLUMNS = [
    "DT_INICIO", "HR_INICIO", "DT_CALC_HE", "DT_INCLUSAO",
    "DT_ALTERACAO", "DT_SUBMISSAO", "DT_APROVACAO_1", "DT_APROVACAO_2",
]
BOOLEAN_COLUMNS = []


def upsert_data(df: pd.DataFrame, table_name: str, csv_file_path: str):
    bulk_upsert(
        df=df, table_name=TABLE_NAME,
        all_columns=TABLE_COLUMNS, pk_columns=PK_COLUMNS,
        date_columns=DATE_COLUMNS, boolean_columns=BOOLEAN_COLUMNS,
        csv_file_path=csv_file_path,
        archive_func=arquivar_csv, archive_name=table_name,
    )
