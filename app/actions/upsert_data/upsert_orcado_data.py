import logging
import os
import pandas as pd
from actions.upsert_data.pg_upsert_utils import bulk_upsert

TABLE_COLUMNS = [
    "CODIGO_PROJETO", "DT_FIM_PROJETO", "DT_INICIO_PROJETO",
    "NOME_PROJETO", "VALOR_PROJETO", "ATIVO", "PROJ_ID",
    "TRABALHO_APONTADO_PROJ", "TRABALHO_FALTANDO_PROJ",
    "TRABALHO_PREVISTO_PROJ", "TRABALHO_REALIZADO_PROJ",
    "DESCRICAO", "NOME_RECURSO", "TX_ID_RECURSO", "TX_RECURSO"
]

TABLE_NAME = "RELATORIO_PSO_ORCADO"
PK_COLUMNS = ["PROJ_ID", "TX_ID_RECURSO"]
DATE_COLUMNS = ["DT_FIM_PROJETO", "DT_INICIO_PROJETO"]
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
