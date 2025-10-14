# process_csv.py
import logging
import pandas as pd
from actions.upsert_realizado_data import TABLE_COLUMNS as TABLE_COLUMNS_REALIZADO
from actions.upsert_orcado_data import TABLE_COLUMNS as TABLE_COLUMNS_ORCADO  
from actions.upsert_planejado_data import TABLE_COLUMNS as TABLE_COLUMNS_PLANEJADO  

def process_csv(file_path: str, script_choice: str):
    logging.info(f"Lendo arquivo CSV: {file_path}...")

    try:

        df = pd.read_csv(file_path, delimiter=";", encoding='latin1')

        if script_choice == "Orçado":
            len_db = len(TABLE_COLUMNS_ORCADO)
        elif script_choice == "Planejado":
            len_db = len(TABLE_COLUMNS_PLANEJADO)
        elif script_choice == "Realizado":
            len_db = len(TABLE_COLUMNS_REALIZADO)
        else:
            raise ValueError(f"Escolha de script inválida: {script_choice}")

        logging.info(f"Arquivo CSV lido com sucesso. Total de colunas: {len(df.columns)}")

        if len(df.columns) != len_db:
            logging.error(f"Erro: o número de colunas no CSV ({len(df.columns)}) não corresponde ao número esperado ({len_db}) para o script '{script_choice}'.")
            raise ValueError(f"O número de colunas no CSV ({len(df.columns)}) não corresponde ao número esperado ({len_db}) para o script '{script_choice}'.")

        return df
    
    except Exception as e:
        logging.error(f"Erro ao processar o arquivo CSV: {e}")
        raise
