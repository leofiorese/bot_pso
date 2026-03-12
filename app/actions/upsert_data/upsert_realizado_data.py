import logging
import os
import pandas as pd
from actions.upsert_data.pg_upsert_utils import bulk_upsert

TABLE_COLUMNS = [
    "APON_ID", "USU_ID", "ATIVO", "EMAIL", "NOME_USUARIO", "ATIV_ID",
    "DT_INICIO_APONTAMENTO", "PROJ_ID", "TX_COLABORADOR",
    "NOME_ATIVIDADE", "B_DT_FIM_ATIVIDADE", "B_DT_INICIO_ATIVIDADE",
    "DT_FIM_ATIVIDADE", "DT_INICIO_ATIVIDADE",
    "DURACAO_PREVISTA_HORAS", "TRABALHO_APONTADO_ATIVIDADE",
    "TRABALHO_FALTANDO_ATIVIDADE", "TRABALHO_PREVISTO_ATIVIDADE",
    "CODIGO_PROJETO", "DT_FIM_PROJETO", "DT_INICIO_PROJETO",
    "NOME_PROJETO", "VALOR_PROJETO", "HORAS", "CUSTO_APONT"
]

TABLE_NAME = "RELATORIO_PSO_REALIZADO"
PK_COLUMNS = ["APON_ID"]
DATE_COLUMNS = [
    "DT_INICIO_APONTAMENTO", "B_DT_FIM_ATIVIDADE",
    "B_DT_INICIO_ATIVIDADE", "DT_FIM_ATIVIDADE",
    "DT_INICIO_ATIVIDADE", "DT_FIM_PROJETO", "DT_INICIO_PROJETO",
]
BOOLEAN_COLUMNS = ["ATIVO"]


def upsert_data(df: pd.DataFrame, table_name: str, csv_file_path: str):
    bulk_upsert(
        df=df, table_name=TABLE_NAME,
        all_columns=TABLE_COLUMNS, pk_columns=PK_COLUMNS,
        date_columns=DATE_COLUMNS, boolean_columns=BOOLEAN_COLUMNS,
    )
    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)
        logging.info(f"Arquivo CSV {csv_file_path} excluido com sucesso.")
