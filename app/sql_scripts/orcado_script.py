# orcado_script.py

import logging

def script_sql(dateadd_value):
    
    return f"""
SELECT
  p.CODIGO AS CODIGO_PROJETO,
  p.DT_FIM AS DT_FIM_PROJETO,
  p.DT_INICIO AS DT_INICIO_PROJETO,
  p.NOME AS NOME_PROJETO,
  p.VALOR AS VALOR_PROJETO,
  P.ATIVO,
  p.PROJ_ID,
  CAST(p.TRABALHO_APONTADO / 60 AS DECIMAL (10,2)) AS TRABALHO_APONTADO_PROJ,
  CAST(p.TRABALHO_FALTANDO / 60 AS DECIMAL (10,2)) AS TRABALHO_FALTANDO_PROJ, 
  CAST(p.TRABALHO_PREVISTO / 60 AS DECIMAL (10,2)) AS TRABALHO_PREVISTO_PROJ,
  CAST(p.TRABALHO_REALIZADO / 60 AS DECIMAL (10,2)) AS TRABALHO_REALIZADO_PROJ,
  pr.DESCRICAO,
  pr.NOME AS NOME_RECURSO,
  pr.TAXA_ID_CUS_PREV AS TX_ID_RECURSO,
  th.VALOR AS TX_RECURSO
FROM
  PSO_PROJETOS p
JOIN
  PSO_PROJ_RECURSOS pr ON p.PROJ_ID = pr.PROJ_ID
JOIN
  PSO_TAXA_HISTORICO th ON pr.TAXA_ID_CUS_PREV = th.TAXA_ID 
WHERE
  p.INCLUIDO_EM BETWEEN DATEADD(day, {dateadd_value}, GETDATE()) AND GETDATE()
  AND p.CODIGO = 'Teste'
  
"""

def gerar_script_final(dateadd_string):
    logging.info(f"rel_script.py: Recebido dateadd_string = '{dateadd_string}', gerando a query.")
    script_completo = script_sql(dateadd_string)

    return script_completo