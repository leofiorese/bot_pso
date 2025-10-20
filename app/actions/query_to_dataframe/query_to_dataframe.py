import pandas as pd
from sqlalchemy import create_engine
from db.db import get_conn
import logging
import os
from dotenv import load_dotenv

load_dotenv()

def get_sqlalchemy_engine():
    host = os.getenv("MYSQL_HOST", "localhost")
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "")
    database = os.getenv("MYSQL_DB", None)
    port = os.getenv("MYSQL_PORT", "3306")

    db_uri = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

    engine = create_engine(db_uri)
    return engine

def query_to_dataframe(query):
    try:
        engine = get_sqlalchemy_engine()

        df = pd.read_sql(query, engine)
        
        logging.info(f"Consulta executada com sucesso! {len(df)} registros carregados.")
        
        return df
    except Exception as e:
        logging.error(f"Erro ao executar a consulta: {e}")
        return None

if __name__ == "__main__":
    query = "SELECT * FROM tabela_apontamentos"

    df = query_to_dataframe(query)

    if df is not None:
        print(df.head())
    else:
        logging.error("Não foi possível carregar os dados.")
