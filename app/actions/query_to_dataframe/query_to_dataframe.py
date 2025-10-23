import pandas as pd
from sqlalchemy import create_engine
from db.db import get_conn
import logging
import os
from dotenv import load_dotenv

load_dotenv()

# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', 1000)

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

        # df_markdown = df.to_markdown(index=False, tablefmt="github")
        
        logging.info(f"Consulta executada com sucesso! {len(df)} registros carregados.")

        return df
    except Exception as e:
        logging.error(f"Erro ao executar a consulta: {e}")
        return None
