import logging
import json
import re  # Importa o módulo de Expressões Regulares
from db.db import get_conn
from decimal import Decimal

logging.getLogger(__name__)

TABLE_NAME = "RELATORIO_PSO_INSIGHTS_LLM"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `RELATORIO_PSO_INSIGHTS_LLM` (
    `id_insight` INT AUTO_INCREMENT NOT NULL,
    `PROJ_ID` INT NOT NULL,
    `CODIGO_PROJETO` VARCHAR(255),
    `VALOR_PROJETO` DECIMAL(20, 2),
    `USU_ID` INT,
    `analise_resumida_json` JSON,
    `insights_acionaveis_md` MEDIUMTEXT,
    `pontos_de_atencao_md` MEDIUMTEXT,
    `recomendacoes_md` MEDIUMTEXT,
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`id_insight`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO RELATORIO_PSO_INSIGHTS_LLM (
    PROJ_ID, CODIGO_PROJETO, VALOR_PROJETO, USU_ID,
    analise_resumida_json,
    insights_acionaveis_md, pontos_de_atencao_md, recomendacoes_md
) VALUES (
    %(PROJ_ID)s, %(CODIGO_PROJETO)s, %(VALOR_PROJETO)s, %(USU_ID)s,
    %(analise_resumida_json)s,
    %(insights_acionaveis_md)s, %(pontos_de_atencao_md)s, %(recomendacoes_md)s
)
ON DUPLICATE KEY UPDATE
    CODIGO_PROJETO = VALUES(CODIGO_PROJETO),
    VALOR_PROJETO = VALUES(VALOR_PROJETO),
    USU_ID = VALUES(USU_ID),
    analise_resumida_json = VALUES(analise_resumida_json),
    insights_acionaveis_md = VALUES(insights_acionaveis_md),
    pontos_de_atencao_md = VALUES(pontos_de_atencao_md),
    recomendacoes_md = VALUES(recomendacoes_md);
"""

def _create_table(cursor, table_name):
    try:
        cursor.execute(CREATE_TABLE_SQL)
        logging.info(f"Tabela {table_name} criada/verificada.")
    except Exception as e:
        logging.error(f"Erro ao criar/verificar a tabela: {e}")
        raise

def _convert_list_to_markdown(item_list: list) -> str:
    if not item_list:
        return None
    return "\n".join(f"- {item.lstrip('*- ')}" for item in item_list)

def _safe_convert_to_decimal(val):
    if val is None:
        return None
    try:
        return Decimal(str(val))
    except Exception:
        return None

def upsert_data(llm_json_response: str):

    conn = None
    cursor = None
    proj_id = None 

    try:
        conn = get_conn()
        cursor = conn.cursor()
        _create_table(cursor, TABLE_NAME)

        logging.info("Processando resposta JSON da LLM para upsert...")
        
        try:
            pattern = re.compile(r"```json\s*(.*?)\s*```", re.DOTALL)
            
            match = pattern.search(llm_json_response)
            
            if not match:
                logging.error(f"Não foi possível encontrar o bloco JSON (```json...```) na resposta. Resposta recebida: {llm_json_response}")
                raise json.JSONDecodeError("Bloco JSON delimitado (```json...```) não encontrado", llm_json_response, 0)
            
            clean_json_string = match.group(1).strip()

            if not clean_json_string:
                 logging.error(f"JSON extraído entre os marcadores está vazio. Resposta recebida: {llm_json_response}")
                 raise json.JSONDecodeError("JSON extraído está vazio", llm_json_response, 0)

            data = json.loads(clean_json_string)

        except json.JSONDecodeError as e:
            logging.error(f"Erro ao decodificar JSON: {e}. Resposta recebida (bruta): {llm_json_response}")
            raise
            
        chaves = data.get('chaves_identificadoras', {})
        
        proj_id = chaves.get('PROJ_ID')
        if not proj_id:
            raise ValueError("JSON da LLM não continha 'PROJ_ID' em 'chaves_identificadoras'. Upsert cancelado.")

        logging.info(f"Preparando dados para o PROJ_ID: {proj_id}")

        analise_resumida_obj = data.get('analise_resumida', {})

        if analise_resumida_obj:
            analise_resumida_str = json.dumps(analise_resumida_obj, ensure_ascii=False)
        
        else:
            analise_resumida_str = None

        insights_md = _convert_list_to_markdown(data.get('insights_acionaveis', []))
        pontos_atencao_md = _convert_list_to_markdown(data.get('pontos_de_atencao', []))
        recomendacoes_md = _convert_list_to_markdown(data.get('recomendacoes', []))

        data_to_upsert = {
            "PROJ_ID": proj_id,
            "CODIGO_PROJETO": chaves.get('CODIGO_PROJETO'),
            "VALOR_PROJETO": _safe_convert_to_decimal(chaves.get('VALOR_PROJETO')),
            "USU_ID": chaves.get('USU_ID'),
            "analise_resumida_json": analise_resumida_str,
            "insights_acionaveis_md": insights_md,
            "pontos_de_atencao_md": pontos_atencao_md,
            "recomendacoes_md": recomendacoes_md
        }

        logging.info(f"Executando UPSERT para o PROJ_ID: {proj_id}...")
        cursor.execute(UPSERT_SQL, data_to_upsert)
        conn.commit()

        logging.info(f"Upsert realizado com sucesso na tabela {TABLE_NAME} para o PROJ_ID: {proj_id}.")
        if analise_resumida_obj:
            logging.info(f"Métricas dinâmicas salvas: {list(analise_resumida_obj.keys())}")

    except Exception as e:
        error_msg = f"Erro no upsert de insights para PROJ_ID {proj_id}: {e}" if proj_id else f"Erro no upsert de insights: {e}"
        logging.error(error_msg)
        
        if conn:
            conn.rollback()
            logging.info("Rollback realizado.")
        
        raise

    finally:
        if cursor:
            cursor.close()
            logging.info("Cursor fechado.")
        
        if conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")