import logging

def script_sql(dateadd_value):
    
    return f"""
SELECT * FROM PSO_CENTROS_RESULTADO WHERE ALTERADO_EM BETWEEN DATEADD(day, {dateadd_value}, GETDATE()) AND GETDATE()  
"""

def gerar_script_final(dateadd_string):
    logging.info(f"rel_script.py: Recebido dateadd_string = '{dateadd_string}', gerando a query.")
    script_completo = script_sql(dateadd_string)

    return script_completo