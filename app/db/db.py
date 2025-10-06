import os
import sys
import mysql.connector as mc
from mysql.connector import Error
from dotenv import load_dotenv

def _load_config():

    load_dotenv()

    return {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "database": os.getenv("MYSQL_DB", None),
        "connection_timeout": 5,
    }

def get_conn():
    cfg = _load_config()

    return mc.connect(**cfg)

def main():
    cfg = _load_config()

    try:
        conn = mc.connect(**cfg)
        conn.ping(reconnect=False, attempts=1, delay=0)
        db = cfg["database"] or "(sem database selecionado)"
        print(f"Conexão bem-sucedida em {cfg['host']}:{cfg['port']} / DB Conectado: {db}")

        conn.close()
        sys.exit(0)

    except Error as e:
        print("Falha na conexão com o MySQL.")
        print(f"Detalhes: {e}")
        
        sys.exit(1)

if __name__ == "__main__":
    main()
