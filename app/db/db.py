import os
import sys
import mysql.connector as mc
from mysql.connector import Error
from dotenv import load_dotenv
import logging

def get_base_path():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    
BASE_PATH = get_base_path()


def _load_config():
    load_dotenv(os.path.join(BASE_PATH, '.env'))

    return {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "database": os.getenv("MYSQL_DB", None),
        "connection_timeout": 5,
    }

def _ensure_database_exists(cfg):
    db_name = cfg.get("database")
    if not db_name:
        return 

    try:
        temp_cfg = cfg.copy()
        temp_cfg.pop("database", None)
        conn = mc.connect(**temp_cfg)
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` DEFAULT CHARACTER SET 'utf8mb4'")
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Banco de dados '{db_name}' garantido (criado se não existia).")
    except Error as e:
        logging.error(f"Erro ao garantir existência do banco '{db_name}': {e}")

def get_conn():
    cfg = _load_config()
    _ensure_database_exists(cfg)

    return mc.connect(**cfg)

def main():
    cfg = _load_config()

    try:
        _ensure_database_exists(cfg)
        conn = mc.connect(**cfg)
        conn.ping(reconnect=False, attempts=1, delay=0)
        db = cfg["database"] or "(sem database selecionado)"
        
        logging.info(f"Conexão com banco de dados bem-sucedida em: {cfg['host']}:{cfg['port']} / DB Conectado: {db}")

        conn.close()
        sys.exit(0)

    except Error as e:
        logging.error("Falha na conexão com o MySQL.")
        logging.error(f"Detalhes: {e}")
        
        sys.exit(1)

if __name__ == "__main__":
    main()
