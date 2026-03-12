import logging
import pandas as pd
from utils import arquivar_csv
from actions.upsert_data.pg_upsert_utils import bulk_upsert

TABLE_COLUMNS = [
    "ATIV_ID", "PROJ_ID", "ATIV_ID_PAI", "EXT_TASK_UID", "NOME",
    "DESCRICAO", "COMENTARIO", "DT_INICIO", "DT_FIM", "DT_FIM_REAL",
    "B_DT_INICIO", "B_DT_FIM", "DT_INICIO_REAL", "TRABALHO_PREVISTO",
    "TRABALHO_REALIZADO", "TRABALHO_FALTANDO", "TRABALHO_APONTADO",
    "LIMITE_MINUTOS", "B_TRABALHO_PREVISTO", "ORDEM", "CALC_ORDEM",
    "CALC_NIVEL", "DURACAO_PREVISTA", "B_DURACAO_PREVISTA",
    "L_DT_INICIO", "L_DT_FIM", "L_DURACAO_PREVISTA",
    "IND_ENCERRADA", "IND_ETAPA", "IND_APROVADA", "IND_APO_BLOQUEADO",
    "TIPO", "WBS", "BCWP", "BCWS", "B_BCWP", "VA_VP", "CR_VC",
    "ESTADO", "IND_OS", "IND_OS_INICIAL", "IND_OS_FINAL",
    "TDE_ID_FILHO", "IND_HORAS_FATURAVEIS", "OS_ID_ATIV",
    "INCLUIDO_EM", "ALTERADO_EM", "ESTADO_OS", "VALOR",
    "VALOR_FATURADO", "PRECEDENTES", "UDF1", "UDF2", "UDF3", "UDF4",
    "UDF5", "UDF6", "UDF7", "UDF8", "UDF9", "UDF10", "AUX1",
    "TRELLO_CARD_ID", "ENDERECO", "ATIV_JIRA_ID", "AGILE_PROJ_ID",
    "AGILE_SPRINT_ID", "AGILE_ISSUE_ID", "AGILE_DT_INICIO",
    "AGILE_DT_FIM", "USU_APROV_ETAPA", "AGILE_EPIC_ID"
]

TABLE_NAME = "ATIVIDADES"
PK_COLUMNS = ["ATIV_ID"]
DATE_COLUMNS = [
    "DT_INICIO", "DT_FIM", "DT_FIM_REAL", "B_DT_INICIO", "B_DT_FIM",
    "DT_INICIO_REAL", "L_DT_INICIO", "L_DT_FIM", "AGILE_DT_INICIO",
    "AGILE_DT_FIM", "INCLUIDO_EM", "ALTERADO_EM",
]
BOOLEAN_COLUMNS = [
    "IND_ENCERRADA", "IND_ETAPA", "IND_APROVADA", "IND_APO_BLOQUEADO",
    "IND_OS", "IND_OS_INICIAL", "IND_OS_FINAL", "IND_HORAS_FATURAVEIS",
]


def upsert_data(df: pd.DataFrame, table_name: str, csv_file_path: str):
    bulk_upsert(
        df=df, table_name=TABLE_NAME,
        all_columns=TABLE_COLUMNS, pk_columns=PK_COLUMNS,
        date_columns=DATE_COLUMNS, boolean_columns=BOOLEAN_COLUMNS,
        csv_file_path=csv_file_path,
        archive_func=arquivar_csv, archive_name=table_name,
    )
