# process_csv.py atualizado para suportar as 21 novas tabelas (e manter Orçado/Planejado/Realizado)
import logging
import pandas as pd

from actions.upsert_data.upsert_realizado_data import TABLE_COLUMNS as TABLE_COLUMNS_REALIZADO
from actions.upsert_data.upsert_orcado_data import TABLE_COLUMNS as TABLE_COLUMNS_ORCADO
from actions.upsert_data.upsert_planejado_data import TABLE_COLUMNS as TABLE_COLUMNS_PLANEJADO

from actions.upsert_data.upsert_agrupamento import TABLE_COLUMNS as TABLE_COLUMNS_AGRUPAMENTO
from actions.upsert_data.upsert_apontamentos import TABLE_COLUMNS as TABLE_COLUMNS_APONTAMENTOS
from actions.upsert_data.upsert_atividades import TABLE_COLUMNS as TABLE_COLUMNS_ATIVIDADES
from actions.upsert_data.upsert_atribuicoes import TABLE_COLUMNS as TABLE_COLUMNS_ATRIBUICOES
from actions.upsert_data.upsert_calendarios import TABLE_COLUMNS as TABLE_COLUMNS_CALENDARIOS
from actions.upsert_data.upsert_centros_de_resultado import TABLE_COLUMNS as TABLE_COLUMNS_CENTROS_DE_RESULTADO
from actions.upsert_data.upsert_d_calend_proj import TABLE_COLUMNS as TABLE_COLUMNS_D_CALEND_PROJ
from actions.upsert_data.upsert_despesa_orcada import TABLE_COLUMNS as TABLE_COLUMNS_DESPESA_ORCADA
from actions.upsert_data.upsert_despesa_tipo import TABLE_COLUMNS as TABLE_COLUMNS_DESPESA_TIPO
from actions.upsert_data.upsert_despesas import TABLE_COLUMNS as TABLE_COLUMNS_DESPESAS
from actions.upsert_data.upsert_empresas import TABLE_COLUMNS as TABLE_COLUMNS_EMPRESAS
from actions.upsert_data.upsert_faturamento import TABLE_COLUMNS as TABLE_COLUMNS_FATURAMENTO
from actions.upsert_data.upsert_grref import TABLE_COLUMNS as TABLE_COLUMNS_GRREF
from actions.upsert_data.upsert_info_colabs import TABLE_COLUMNS as TABLE_COLUMNS_INFO_COLABS
from actions.upsert_data.upsert_projetos import TABLE_COLUMNS as TABLE_COLUMNS_PROJETOS
from actions.upsert_data.upsert_pso_taxa import TABLE_COLUMNS as TABLE_COLUMNS_PSO_TAXA
from actions.upsert_data.upsert_pso_usu_funcoes import TABLE_COLUMNS as TABLE_COLUMNS_PSO_USU_FUNCOES
from actions.upsert_data.upsert_recursos import TABLE_COLUMNS as TABLE_COLUMNS_RECURSOS
from actions.upsert_data.upsert_resumo_de_horas_ativ import TABLE_COLUMNS as TABLE_COLUMNS_RESUMO_DE_HORAS_ATIV
from actions.upsert_data.upsert_resumo_de_horas import TABLE_COLUMNS as TABLE_COLUMNS_RESUMO_DE_HORAS
from actions.upsert_data.upsert_taxa_historico import TABLE_COLUMNS as TABLE_COLUMNS_TAXA_HISTORICO


TABLE_MAP = {
    # existentes
    "Orçado": TABLE_COLUMNS_ORCADO,
    "Planejado": TABLE_COLUMNS_PLANEJADO,
    "Realizado": TABLE_COLUMNS_REALIZADO,

    # novas tabelas
    "AGRUPAMENTO": TABLE_COLUMNS_AGRUPAMENTO,
    "APONTAMENTOS": TABLE_COLUMNS_APONTAMENTOS,
    "ATIVIDADES": TABLE_COLUMNS_ATIVIDADES,
    "ATRIBUICOES": TABLE_COLUMNS_ATRIBUICOES,
    "CALENDARIOS": TABLE_COLUMNS_CALENDARIOS,
    "CENTROS_DE_RESULTADO": TABLE_COLUMNS_CENTROS_DE_RESULTADO,
    "D_CALEND_PROJ": TABLE_COLUMNS_D_CALEND_PROJ,
    "DESPESA_ORCADA": TABLE_COLUMNS_DESPESA_ORCADA,
    "DESPESA_TIPO": TABLE_COLUMNS_DESPESA_TIPO,
    "DESPESAS": TABLE_COLUMNS_DESPESAS,
    "EMPRESAS": TABLE_COLUMNS_EMPRESAS,
    "FATURAMENTO": TABLE_COLUMNS_FATURAMENTO,
    "GRREF": TABLE_COLUMNS_GRREF,
    "INFO_COLABS": TABLE_COLUMNS_INFO_COLABS,
    "PROJETOS": TABLE_COLUMNS_PROJETOS,
    "PSO_TAXA": TABLE_COLUMNS_PSO_TAXA,
    "PSO_USU_FUNCOES": TABLE_COLUMNS_PSO_USU_FUNCOES,
    "RECURSOS": TABLE_COLUMNS_RECURSOS,
    "RESUMO_DE_HORAS_ATIV": TABLE_COLUMNS_RESUMO_DE_HORAS_ATIV,
    "RESUMO_DE_HORAS": TABLE_COLUMNS_RESUMO_DE_HORAS,
    "TAXA_HISTORICO": TABLE_COLUMNS_TAXA_HISTORICO,
}


def process_csv(file_path: str, script_choice: str):
    logging.info(f"Lendo arquivo CSV: {file_path}...")

    try:
        df = pd.read_csv(file_path, delimiter=";", encoding="latin1")

        if script_choice not in TABLE_MAP:
            raise ValueError(f"Escolha de script inválida: {script_choice}")

        len_db = len(TABLE_MAP[script_choice])

        logging.info(f"Arquivo CSV lido com sucesso. Total de colunas: {len(df.columns)}")

        if len(df.columns) != len_db:
            logging.error(
                f"Erro: o número de colunas no CSV ({len(df.columns)}) não corresponde ao número esperado ({len_db}) para '{script_choice}'."
            )
            raise ValueError(
                f"O número de colunas no CSV ({len(df.columns)}) não corresponde ao número esperado ({len_db}) para '{script_choice}'."
            )

        return df

    except Exception as e:
        logging.error(f"Erro ao processar o arquivo CSV: {e}")
        raise
