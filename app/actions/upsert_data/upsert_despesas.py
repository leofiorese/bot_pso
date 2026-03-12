import logging
import pandas as pd
from utils import arquivar_csv
from actions.upsert_data.pg_upsert_utils import bulk_upsert

TABLE_COLUMNS = [
    "DESP_ID", "PROJ_ID", "CR_ID", "DESPT_ID", "RESDESP_ID", "TIPO",
    "TIPO_RESUMO", "USU_ID", "USU_ID_APROVADOR", "USU_ID_APROVACAO_1",
    "USU_ID_APROVACAO_2", "USU_ID_REJEICAO_1", "USU_ID_REJEICAO_2",
    "IND_APROVADO", "DT_DATA", "DESCRICAO", "VALOR", "VALOR_FATURADO",
    "VALOR_RECONHECIDO", "REEMBOLSAVEL", "COBRAVEL", "STATUS",
    "IND_DCTO_ANEXO", "FAT_ID", "PAG_ID", "INCLUIDO_EM", "ALTERADO_EM",
    "DT_SUBMISSAO", "DT_APRREG_1", "DT_APRREG_2", "DT_APROVACAO",
    "AUX1", "DESP_UUID", "VALOR_ORCADO", "VLR_APTO",
    "VLR_APTO_FATURADO", "VLR_APTO_RECONHECIDO", "VLR_APTO_ORCADO"
]

TABLE_NAME = "DESPESAS"
PK_COLUMNS = ["DESP_ID"]
DATE_COLUMNS = [
    "DT_DATA", "INCLUIDO_EM", "ALTERADO_EM", "DT_SUBMISSAO",
    "DT_APRREG_1", "DT_APRREG_2", "DT_APROVACAO",
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
