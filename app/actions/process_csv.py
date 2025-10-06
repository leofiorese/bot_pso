# app/panda_actions/process_csv.py
import logging
import pandas as pd

def process_csv(file_path: str) -> pd.DataFrame:
    """
    Processa o arquivo CSV, garantindo que ele tenha cabeçalhos adequados e o formato correto.
    """
    logging.info(f"Lendo arquivo CSV: {file_path}...")

    try:
        # Verificar as primeiras linhas do CSV para diagnóstico
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = [f.readline() for _ in range(5)]  # Lê as primeiras 5 linhas
            logging.info(f"Primeiras 5 linhas do arquivo CSV:\n{''.join(lines)}")

        # Tenta ler o CSV com um delimitador comum (`,` ou `;`)
        df = pd.read_csv(file_path, delimiter=";", encoding='utf-8')

        # Verifica o número de colunas lidas
        logging.info(f"Arquivo CSV lido com sucesso. Total de colunas: {len(df.columns)}")

        # Verifica se o número de colunas corresponde ao esperado (35)
        if len(df.columns) != 35:
            logging.error(f"Erro: o número de colunas no CSV ({len(df.columns)}) não corresponde ao número esperado (35).")
            raise ValueError(f"O número de colunas no CSV ({len(df.columns)}) não corresponde ao número esperado (35).")

        # Exibe as primeiras 5 linhas do DataFrame para validação
        logging.info(f"Primeiras 5 linhas do DataFrame:\n{df.head()}")

        return df
    
    except Exception as e:
        logging.error(f"Erro ao processar o arquivo CSV: {e}")
        raise
