import os
import sys
import atexit
import logging
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2 import OperationalError
from dotenv import load_dotenv


def get_base_path():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

BASE_PATH = get_base_path()

_pool = None


def _load_config():
    load_dotenv(os.path.join(BASE_PATH, '.env'))
    return {
        "host": os.getenv("DB_HOST", ""),
        "port": int(os.getenv("DB_PORT", "5432")),
        "user": os.getenv("DB_USER", "pmo_admin"),
        "password": os.getenv("DB_PASSWORD", ""),
        "dbname": os.getenv("DB_NAME", "pmo_hub"),
    }


def _get_pool():
    global _pool
    if _pool is None or _pool.closed:
        cfg = _load_config()
        pool_min = int(os.getenv("DB_POOL_MIN", "2"))
        pool_max = int(os.getenv("DB_POOL_MAX", "5"))
        _pool = SimpleConnectionPool(
            minconn=pool_min,
            maxconn=pool_max,
            **cfg
        )
        atexit.register(_close_pool)
        logging.info(
            f"Pool PostgreSQL criado: {cfg['host']}:{cfg['port']}/{cfg['dbname']} "
            f"(min={pool_min}, max={pool_max})"
        )
    return _pool


def get_conn():
    """Obtém conexão do pool. O chamador DEVE chamar put_conn() ao finalizar."""
    pool = _get_pool()
    conn = pool.getconn()
    cursor = conn.cursor()
    cursor.execute("SET search_path TO psoffice, public")
    cursor.close()
    return conn


def put_conn(conn):
    """Devolve conexão ao pool."""
    pool = _get_pool()
    if conn and not conn.closed:
        pool.putconn(conn)


def _close_pool():
    global _pool
    if _pool is not None and not _pool.closed:
        _pool.closeall()
        logging.info("Pool PostgreSQL fechado.")
        _pool = None


def main():
    """Testa conectividade com o PostgreSQL."""
    logging.basicConfig(level=logging.INFO)
    cfg = _load_config()
    try:
        conn = psycopg2.connect(**cfg)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        logging.info(
            f"Conexao com PostgreSQL bem-sucedida: "
            f"{cfg['host']}:{cfg['port']}/{cfg['dbname']}"
        )
        sys.exit(0)
    except OperationalError as e:
        logging.error(f"Falha na conexao com PostgreSQL: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
