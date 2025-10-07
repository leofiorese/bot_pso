# app/panda_actions/process_csv.py
import logging
import pandas as pd
from upsert_data import TABLE_COLUMNS   

def process_csv(file_path: str) -> pd.DataFrame:
    logging.info(f"Lendo arquivo CSV: {file_path}...")

    try:
    
        df = pd.read_csv(file_path, delimiter=";", encoding='latin1')

        len_db = len(TABLE_COLUMNS)

        logging.info(f"Arquivo CSV lido com sucesso. Total de colunas: {len(df.columns)}")

        if len(df.columns) != 36:
            logging.error(f"Erro: o número de colunas no CSV ({len(df.columns)}) não corresponde ao número esperado ({len_db}).")
            raise ValueError(f"O número de colunas no CSV ({len(df.columns)}) não corresponde ao número esperado ({len_db}).")

        return df
    
    except Exception as e:
        logging.error(f"Erro ao processar o arquivo CSV: {e}")
        raise
